require "worker_roulette/version"
require 'oj'
require 'redis'
require 'hiredis'
require 'em-hiredis'
require 'connection_pool'
require "digest/sha1"

Dir[File.join(File.dirname(__FILE__),'worker_roulette','**','*.rb')].sort.each { |file| require file.gsub(/.rb$/, "") }

module WorkerRoulette
  class WorkerRoulette
    JOB_BOARD = "job_board"
    JOB_NOTIFICATIONS = "new_job_ready"
    DEFAULT_POLLING_TIME = 2
    DEFAULT_REDIS_CONFIG = {
      host: 'localhost',
      port: 6379,
      db: 14,
      driver: :hiredis,
      timeout: 5,
      evented: false,
      pool_size: 10,
      polling_time: DEFAULT_POLLING_TIME
    }

    def self.dump(obj)
      Oj.dump(obj)
    rescue Oj::ParseError => e
      {'error' => e, 'unparsable_string' => obj}
    end

    def self.load(json)
      Oj.load(json)
    rescue Oj::ParseError => e
      {'error' => e, 'unparsable_string' => obj}
    end

    def self.job_board_key(namespace = nil)
      "#{namespace + ':' if namespace}#{WorkerRoulette::JOB_BOARD}"
    end

    def self.sender_key(sender, namespace = nil)
      "#{namespace + ':' if namespace}#{sender}"
    end

    def self.counter_key(sender, namespace = nil)
      "#{namespace + ':' if namespace}counter_key"
    end

    def self.start(config = {})
      new(config)
    end

    private_class_method :new
    attr_reader :preprocessors

    def initialize(config = {})
      config.recursive_symbolize_keys!
      @redis_config               = DEFAULT_REDIS_CONFIG.merge(config)
      @pool_config                = { size: @redis_config.delete(:pool_size), timeout: @redis_config.delete(:timeout) }
      @evented                    = @redis_config.delete(:evented)
      @polling_time               = @redis_config.delete(:polling_time)

      @foreman_connection_pool    = ConnectionPool.new(@pool_config) {new_redis}
      @tradesman_connection_pool  = ConnectionPool.new(@pool_config) {new_redis}

      @preprocessors = []

      configure_queue_tracker(config.delete(:metric_tracker))
    end

    def configure_queue_tracker(config)
      return unless config

      QueueMetricTracker.configure(
        {
          server_name: `hostname`.chomp,
          granularity: config[:granularity],
          metric_host: config[:metric_host],
          metric_host_port: config[:metric_host_port],
          metrics: config[:metrics]
        }
      )

      preprocessors << QueueLatency
    end

    def foreman(sender, namespace = nil)
      raise "WorkerRoulette not Started" unless @foreman_connection_pool
      Foreman.new(@foreman_connection_pool, sender, namespace, preprocessors)
    end

    def tradesman(namespace = nil, polling_time = DEFAULT_POLLING_TIME)
      raise "WorkerRoulette not Started" unless @tradesman_connection_pool
      Tradesman.new(@tradesman_connection_pool, @evented, namespace, polling_time || @polling_time, preprocessors)
    end

    def tradesman_connection_pool
      @tradesman_connection_pool
    end

    def pool_size
      (@pool_config ||= {})[:size]
    end

    def redis_config
      (@redis_config ||= {}).dup
    end

    def polling_time
      @polling_time
    end

    private

    def new_redis
      if @evented
        require 'eventmachine'
        redis = EM::Hiredis::Client.new(@redis_config[:host], @redis_config[:port], @redis_config[:password], @redis_config[:db])
        redis.connect
      else
        Redis.new(@redis_config)
      end
    end
  end
end
