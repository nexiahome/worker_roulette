require_relative '../spec_helper'
require 'benchmark'

REDIS_CONNECTION_POOL_SIZE = 100
ITERATIONS = 10_000
SUBSCRIBER_POOL_SIZE = 100

message = ["hello", 'operator']
namespace = 'switchboard_perf_test'

Switchboard.start(REDIS_CONNECTION_POOL_SIZE)
Switchboard.pooled_redis_client.flushdb

puts "Redis Connection Pool Size: #{REDIS_CONNECTION_POOL_SIZE}"

Benchmark.bmbm do |x|
  x.report "Time for #{ITERATIONS} operators to insert #{ITERATIONS * message.length} messages" do
    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      operator = Switchboard.operator(namespace, sender)
      operator.enqueue(message)
    end
  end
end

Benchmark.bmbm do |x|
  x.report "Time for #{ITERATIONS} subscribers to read #{ITERATIONS * 2} messages" do
    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      subscriber = Switchboard.subscriber(namespace)
      subscriber.messages!
    end
  end
end

Switchboard.pooled_redis_client.flushdb

Benchmark.bmbm do |x|

  x.report "Time for #{SUBSCRIBER_POOL_SIZE} subscribers to read #{ITERATIONS * message.length} messages" do
      ITERATIONS.times do |iteration|
        p -> do
          sender = 'sender_' + iteration.to_s
          operator = Switchboard.operator(namespace, sender)
          operator.enqueue(message)
        end
      subscriber = Switchboard.subscriber(namespace)
      subscriber.wait_for_messages(p) {|m| m}
    end
  end
end


Switchboard.pooled_redis_client.flushdb