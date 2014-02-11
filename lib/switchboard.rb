require "switchboard/version"
require 'redis'
require 'hiredis'
require 'redis/connection/hiredis'
require 'redis-namespace'
require 'redis/pool'
require 'oj'

Dir[File.join(File.dirname(__FILE__),'switchboard','**','*.rb')].sort.each { |file| require file.gsub(".rb", "")}

module Switchboard
  JOB_BOARD = "job_board"
  JOB_NOTIFICATIONS = "new_job_ready"

  def self.start(pool_size, config = {host: 'localhost'})
    @operator_connection_pool    = Redis::Pool.new(config.merge Hash[size: pool_size])
    @subscriber_connection_pool  = Redis::Pool.new(config.merge Hash[size: pool_size])
    @pubsub_connection_pool      = Redis::Pool.new(config.merge Hash[size: pool_size])
  end

  def self.operator(namespace, sender)
    raise "Switchboard not Started" unless @operator_connection_pool
    Operator.new(namespace, sender, @operator_connection_pool)
  end

  def self.subscriber(namespace)
    raise "Switchboard not Started" unless @subscriber_connection_pool
    Subscriber.new(namespace, @subscriber_connection_pool, @pubsub_connection_pool)
  end

  def self.subscriber_connection_pool
    @subscriber_connection_pool
  end
end
