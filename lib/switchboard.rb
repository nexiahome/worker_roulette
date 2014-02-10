require "switchboard/version"
require 'redis'
require 'redis-namespace'
require 'redis/pool'
require 'oj'

Dir[File.join(File.dirname(__FILE__),'switchboard','**','*.rb')].sort.each { |file| require file.gsub(".rb", "")}

module Switchboard
  JOB_BOARD = "job_board"
  JOB_NOTIFICATIONS = "new_job_ready"

  def self.start(pool_size, config = {host: 'localhost'})
    @@pooled_redis_clients     = Redis::Pool.new(config.merge Hash[size: pool_size])
    @@pooled_redis_subscribers = Redis::Pool.new(config.merge Hash[size: pool_size])
  end

  def self.operator(namespace, sender)
    raise "Switchboard not Started" unless @@pooled_redis_clients
    Operator.new(namespace, sender, @@pooled_redis_clients)
  end

  def self.subscriber(namespace)
    raise "Switchboard not Started" unless @@pooled_redis_clients
    Subscriber.new(namespace, @@pooled_redis_clients, @@pooled_redis_subscribers)
  end
end
