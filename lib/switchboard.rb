require "switchboard/version"
require 'redis'
require 'hiredis'
require 'redis/connection/hiredis'
require 'connection_pool'
require 'oj'

Dir[File.join(File.dirname(__FILE__),'switchboard','**','*.rb')].sort.each { |file| require file.gsub(".rb", "")}

module Switchboard
  JOB_BOARD = "job_board"
  JOB_NOTIFICATIONS = "new_job_ready"

  def self.start(pool_size = 10, config = {})
    config = {host: 'localhost', db: 'switchboard', timeout: 5}.merge(config)
    @operator_connection_pool    = ConnectionPool.new(config.merge Hash[size: pool_size, timeout: config[:timeout]]) {Redis.new}
    @subscriber_connection_pool  = ConnectionPool.new(config.merge Hash[size: pool_size, timeout: config[:timeout]]) {Redis.new}
    @pubsub_connection_pool      = ConnectionPool.new(config.merge Hash[size: pool_size, timeout: config[:timeout]]) {Redis.new}
  end

  def self.operator(sender)
    raise "Switchboard not Started" unless @operator_connection_pool
    Operator.new(sender, @operator_connection_pool)
  end

  def self.subscriber
    raise "Switchboard not Started" unless @subscriber_connection_pool
    Subscriber.new(@subscriber_connection_pool, @pubsub_connection_pool)
  end

  def self.subscriber_connection_pool
    @subscriber_connection_pool
  end

  def self.pubsub_connection_pool
    @pubsub_connection_pool
  end
end
