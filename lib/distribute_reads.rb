require "makara"
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

    begin
    connection ||= ActiveRecord::Base.connection

    replica_pool = connection.instance_variable_get(:@slave_pool)
    if replica_pool && replica_pool.connections.size > 1
      warn "[distribute_reads] Multiple replicas available, lag only reported for one"
    end

    if %w(PostgreSQL PostGIS).include?(connection.adapter_name)
      # cache the version number
      @server_version_num ||= {}
      cache_key = connection.pool.object_id
      @server_version_num[cache_key] ||= connection.execute("SHOW server_version_num").first["server_version_num"].to_i

      lag_condition =
        if @server_version_num[cache_key] >= 100000
          "pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn()"
        else
          "pg_last_xlog_receive_location() = pg_last_xlog_replay_location()"
        end

      connection.execute(
        "SELECT CASE
          WHEN NOT pg_is_in_recovery() OR #{lag_condition} THEN 0
          ELSE EXTRACT (EPOCH FROM NOW() - pg_last_xact_replay_timestamp())
        END AS lag".squish
      ).first["lag"].to_f
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
    rescue ActiveRecord::StatementInvalid => e
      if e.original_exception.is_a?(PG::FeatureNotSupported)
        # AWS Aurora raises the following error when checking the replication lag:
        # Function pg_last_xlog_receive_location() is currently not supported for Aurora
        # This makes sense given the way replication is handled in Aurora.
        # For now the solution is to return 0 lag.
        return 0.5
      else
        raise e
  end
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
