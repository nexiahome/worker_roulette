require "worker_roulette/version"
require 'oj'
require 'redis'
require 'hiredis'
require 'em-hiredis'
require 'connection_pool'
require "digest/sha1"

Dir[File.join(File.dirname(__FILE__),'worker_roulette','**','*.rb')].sort.each { |file| require file.gsub(".rb", "")}

module WorkerRoulette
  JOB_BOARD = "job_board"
  JOB_NOTIFICATIONS = "new_job_ready"

  def self.start(config = {})
    @redis_config               = {host: 'localhost', port: 6379, db: 14, driver: :hiredis, timeout: 5, evented: false, pool_size: 10}.merge(config)
    @pool_config                = Hash[size: @redis_config.delete(:pool_size), timeout: @redis_config.delete(:timeout)]
    @evented                    = @redis_config.delete(:evented)

    @foreman_connection_pool    = ConnectionPool.new(@pool_config) {new_redis}
    @tradesman_connection_pool  = ConnectionPool.new(@pool_config) {new_redis}
    @pubsub_connection_pool     = ConnectionPool.new(@pool_config) {new_redis_pubsub}
  end

  def self.foreman(sender, channel = nil)
    raise "WorkerRoulette not Started" unless @foreman_connection_pool
    Foreman.new(sender, @foreman_connection_pool, channel)
  end

  def self.tradesman(channel = nil)
    raise "WorkerRoulette not Started" unless @tradesman_connection_pool
    Tradesman.new(@tradesman_connection_pool, @pubsub_connection_pool, channel)
  end

  def self.a_foreman(sender, channel = nil)
    raise "WorkerRoulette not Started" unless @foreman_connection_pool
    AForeman.new(sender, @foreman_connection_pool, channel)
  end

  def self.a_tradesman(channel = nil)
    raise "WorkerRoulette not Started" unless @tradesman_connection_pool
    ATradesman.new(@tradesman_connection_pool, @pubsub_connection_pool, channel)
  end

  def self.tradesman_connection_pool
    @tradesman_connection_pool
  end

  def self.pubsub_connection_pool
    @pubsub_connection_pool
  end

  def self.pool_size
    @pool_config[:size]
  end

  def self.redis_config
    @redis_config.dup
  end

private
  def self.new_redis
    if @evented
      require 'eventmachine'
      redis = EM::Hiredis::Client.new(@redis_config[:host], @redis_config[:port], @redis_config[:password], @redis_config[:db])
      redis.connect
    else
      Redis.new(@redis_config)
    end
  end

  def self.new_redis_pubsub
    if @evented
     new_redis.pubsub
    else
      new_redis
    end
  end
end
