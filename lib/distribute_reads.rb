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

  def self.lag(connection: nil)
    raise DistributeReads::Error, "Don't use outside distribute_reads" unless Thread.current[:distribute_reads]

    connection ||= ActiveRecord::Base.connection
    if %w(PostgreSQL PostGIS).include?(connection.adapter_name)
      replica_pool = connection.instance_variable_get(:@slave_pool)
      if replica_pool && replica_pool.connections.size > 1
        warn "[distribute_reads] Multiple replicas available, lag only reported for one"
      end

      connection.execute(
        "SELECT CASE
          WHEN NOT pg_is_in_recovery() OR pg_last_xlog_receive_location() = pg_last_xlog_replay_location() THEN 0
          ELSE EXTRACT (EPOCH FROM NOW() - pg_last_xact_replay_timestamp())
        END AS lag"
      ).first["lag"].to_f
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
      Rails.logger.debug 'Distributing reads!'
      Rails.logger.debug '*'*80
      value = yield
      Rails.logger.debug '*'*80
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
