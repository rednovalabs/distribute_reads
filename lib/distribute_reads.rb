require "makara"
require "concurrent"
require "distribute_reads/appropriate_pool"
require "distribute_reads/cache_store"
require "distribute_reads/version"

module DistributeReads
  class Error < StandardError; end
  class TooMuchLag < Error; end
  class NoReplicasAvailable < Error; end

  class << self
    attr_accessor :by_default
    attr_accessor :default_options
  end
  @@aurora = Concurrent::AtomicFixnum.new(0)
  self.by_default = false
  self.default_options = {
    failover: true,
    lag_failover: false
  }

  def self.replication_lag(connection: nil)
    distribute_reads do
      lag(connection: connection)
    end
  end

  def self.lag(connection: nil)
    raise DistributeReads::Error, "Don't use outside distribute_reads" unless Thread.current[:distribute_reads]

    connection ||= ActiveRecord::Base.connection
    replica_pool = connection.instance_variable_get(:@slave_pool)
    if replica_pool && replica_pool.connections.size > 1
      warn "[distribute_reads] Multiple replicas available, lag only reported for one"
    end

    if %w(PostgreSQL PostGIS).include?(connection.adapter_name)
      if @@aurora.value == 0
        # test for aurora
        begin
          connection.execute("SELECT AURORA_VERSION();")
          @@aurora.compare_and_set(0, 1)
        rescue ActiveRecord::StatementInvalid
          @@aurora.compare_and_set(0, -1)
        end
      end

      case @@aurora.value
      when -1
        # not on aurora
        # cache the version number
        @server_version_num ||= {}
        cache_key = connection.pool.object_id
        @server_version_num[cache_key] ||= connection.execute("SHOW server_version_num").first["server_version_num"].to_i

        lag_condition = if @server_version_num[cache_key] >= 100000
          "pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn()"
        else
          "pg_last_xlog_receive_location() = pg_last_xlog_replay_location()"
        end

        lag_query = <<-SQL
SELECT CASE
WHEN NOT pg_is_in_recovery() OR #{lag_condition} THEN 0
ELSE EXTRACT (EPOCH FROM NOW() - pg_last_xact_replay_timestamp())
END AS lag
SQL

        connection.execute(lag_query.squish).first["lag"].to_f
      when 1
        # yes on aurora
        # AWS Aurora does not use traditional PG replication.
        # See https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/AuroraPostgreSQL.Replication.html
        # We could query the API every so often, but in practice the lag is so low it would be a terrible overhead to check it. Give a reasonable approximation of the replica lag
        return 0.5
      end
    elsif %w(MySQL Mysql2 Mysql2Spatial Mysql2Rgeo).include?(connection.adapter_name)
      replica_value = Thread.current[:distribute_reads][:replica]
      begin
        # makara doesn't send SHOW queries to replica, so we must force it
        Thread.current[:distribute_reads][:replica] = true
        status = connection.exec_query("SHOW SLAVE STATUS").to_hash.first
        status ? status["Seconds_Behind_Master"].to_f : 0.0
      ensure
        Thread.current[:distribute_reads][:replica] = replica_value
      end
    else
      raise DistributeReads::Error, "Option not supported with this adapter"
    end
  end

  def self.distribute_reads(**options)
    raise ArgumentError, "Missing block" unless block_given?

    unknown_keywords = options.keys - [:failover, :lag_failover, :lag_on, :max_lag, :primary]
    raise ArgumentError, "Unknown keywords: #{unknown_keywords.join(", ")}" if unknown_keywords.any?

    options = DistributeReads.default_options.merge(options)

    previous_value = Thread.current[:distribute_reads]
    begin
      Thread.current[:distribute_reads] = {failover: options[:failover], primary: options[:primary]}

      # TODO ensure same connection is used to test lag and execute queries
      max_lag = options[:max_lag]
      if max_lag && !options[:primary]
        Array(options[:lag_on] || [ActiveRecord::Base]).each do |base_model|
          if DistributeReads.lag(connection: base_model.connection) > max_lag
            if options[:lag_failover]
              # TODO possibly per connection
              Thread.current[:distribute_reads][:primary] = true
            else
              raise DistributeReads::TooMuchLag, "Replica lag over #{max_lag} seconds#{options[:lag_on] ? " on #{base_model.name} connection" : ""}"
            end
          end
        end
      end

      value = yield
      warn "[distribute_reads] Call `to_a` inside block to execute query on replica" if value.is_a?(ActiveRecord::Relation) && !previous_value
      value
    ensure
      Thread.current[:distribute_reads] = previous_value
    end
  end
end

Makara::Proxy.send :prepend, DistributeReads::AppropriatePool
#
# ActiveSupport.on_load(:active_job) do
#   require "distribute_reads/job_methods"
#   include DistributeReads::JobMethods
# end
