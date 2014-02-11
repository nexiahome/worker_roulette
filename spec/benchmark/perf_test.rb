require_relative '../spec_helper'
require 'benchmark'
require 'eventmachine'

REDIS_CONNECTION_POOL_SIZE = 100
ITERATIONS = 10_000

message = {'ding dong' => "hello_operator_" * 100}

Switchboard.start(REDIS_CONNECTION_POOL_SIZE)#{driver: :synchrony}
Switchboard.subscriber_connection_pool.with {|r| r.flushdb}

puts "Redis Connection Pool Size: #{REDIS_CONNECTION_POOL_SIZE}"

Benchmark.bmbm do |x|
  x.report "Time to insert and read #{ITERATIONS} large messages" do # ~2500 messages / second round trip; 50-50 read-write time; CPU and IO bound
    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      operator = Switchboard.operator(sender)
      operator.enqueue(message)
    end

    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      subscriber = Switchboard.subscriber
      subscriber.messages!
    end
  end
end

Switchboard.subscriber_connection_pool.with {|r| r.flushdb}

Benchmark.bmbm do |x|
  x.report "Time for subscribers to enqueue and read #{ITERATIONS} large messages via pubsub" do # ~1800 messages / second round trip
      ITERATIONS.times do |iteration|
        p = -> do
          sender = 'sender_' + iteration.to_s
          operator = Switchboard.operator(sender)
          operator.enqueue(message)
        end
      subscriber = Switchboard.subscriber
      subscriber.wait_for_messages(p) {|m| m}
    end
  end
end

Switchboard.subscriber_connection_pool.with {|r| r.flushdb}

# EM.run do
#   EM.add_timer(6) {puts "em off";EM.stop}
#   subscribers = []
#   operators = []
#   @start = Time.now
#   @end = nil
#   ITERATIONS.times do |iteration|
#     s = Switchboard.subscriber
#     subscribers << s
#     sender = 'sender_' + iteration.to_s
#     operator = Switchboard.operator(sender)
#     a = -> {operator.enqueue(message)}
#     s.wait_for_messages(a) {|m| @end = Time.now if iteration == (ITERATIONS - 1) }
#   end
# end

# puts  @end - @start
# Switchboard.subscriber_connection_pool.with {|r| r.flushdb}