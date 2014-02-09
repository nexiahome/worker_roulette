require "switchboard/version"
require 'redis'
require 'redis-namespace'
require 'redis-pool'

Dir[File.join(File.dirname(__FILE__),'switchboard','**','*.rb')].sort.each { |file| require file.gsub(".rb", "")}

module Switchboard
  JOB_BOARD = "job_board"
  JOB_NOTIFICATIONS = "new_job_ready"

  def self.start(default_namespace, pool_size, config = {host: 'localhost'})
    @@default_namespace = default_namespace
    @@pooled_redis_clients     = Redis::Pool.new(config.merge({size: pool_size})
    @@pooled_redis_subscribers = Redis::Pool.new(config.merge({size: pool_size})
  end

  def self.operator(sender, namespace = nil)
    raise "Switchboard not Started" unless @@default_namespace
    namespace ||= @@default_namespace
    Operator.new(namespace, sender, @@pooled_redis_clients)
  end

  def self.subscriber(namespace = nil)
    raise "Switchboard not Started" unless @@default_namespace
    namespace ||= @@default_namespace
    Subscriber.new(namespace, sender, @@pooled_redis_clients, @@pooled_redis_subscribers)
  end
end
