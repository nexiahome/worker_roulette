require "worker_roulette/version"
require 'redis'
require 'hiredis'
require 'redis/connection/hiredis'
require 'connection_pool'
require 'oj'

Dir[File.join(File.dirname(__FILE__),'worker_roulette','**','*.rb')].sort.each { |file| require file.gsub(".rb", "")}

module WorkerRoulette
  JOB_BOARD = "job_board"
  JOB_NOTIFICATIONS = "new_job_ready"

  def self.start(pool_size = 10, config = {})
    config = {host: 'localhost', db: 15, timeout: 5}.merge(config)
    @foreman_connection_pool    = ConnectionPool.new(config.merge Hash[size: pool_size, timeout: config[:timeout]]) {Redis.new}
    @tradesman_connection_pool  = ConnectionPool.new(config.merge Hash[size: pool_size, timeout: config[:timeout]]) {Redis.new}
    @pubsub_connection_pool     = ConnectionPool.new(config.merge Hash[size: pool_size, timeout: config[:timeout]]) {Redis.new}
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
end
