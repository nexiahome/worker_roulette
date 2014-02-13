require "worker_roulette/version"
require 'redis'
require 'hiredis'
require 'oj'
require 'em-synchrony'

Dir[File.join(File.dirname(__FILE__),'worker_roulette','**','*.rb')].sort.each { |file| require file.gsub(".rb", "")}

class EventMachine::Synchrony::ConnectionPool
  alias_method :with, :execute
end

module WorkerRoulette
  JOB_BOARD = "job_board"
  JOB_NOTIFICATIONS = "new_job_ready"

  def self.start(config = {})
    @redis_config               = {host: 'localhost', db: 14, driver: :hiredis, timeout: 5, pool_size: 10}.merge(config)
    @pool_config                = Hash[size: @redis_config.delete(:pool_size), timeout: @redis_config.delete(:timeout)]
    @foreman_connection_pool    = connection_pool.new(@pool_config) {Redis.new(@redis_config)}
    @tradesman_connection_pool  = connection_pool.new(@pool_config) {Redis.new(@redis_config)}
    @pubsub_connection_pool     = connection_pool.new(@pool_config) {Redis.new(@redis_config)}
  end

  def self.foreman(sender)
    raise "WorkerRoulette not Started" unless @foreman_connection_pool
    Foreman.new(sender, @foreman_connection_pool)
  end

  def self.tradesman
    raise "WorkerRoulette not Started" unless @tradesman_connection_pool
    Tradesman.new(@tradesman_connection_pool, @pubsub_connection_pool)
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
  def connection_pool
    if @redis_config[:driver] == :synchrony
      require 'redis/connection/synchrony'
      require 'em-synchrony/connection_pool'
      EM::Synchrony::ConnectionPool
    else
      require 'redis/connection/hiredis'
      require 'connection_pool'
      ConnectionPool
    end
  end
end
