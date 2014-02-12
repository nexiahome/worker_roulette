require_relative '../spec_helper'
require 'benchmark'
require 'eventmachine'

REDIS_CONNECTION_POOL_SIZE = 100
ITERATIONS = 10_000

work_order = {'ding dong' => "hello_foreman_" * 100}

WorkerRoulette.start(REDIS_CONNECTION_POOL_SIZE)#{driver: :synchrony}
WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

puts "Redis Connection Pool Size: #{REDIS_CONNECTION_POOL_SIZE}"

Benchmark.bmbm do |x|
  x.report "Time to insert and read #{ITERATIONS} large work_orders" do # ~2500 work_orders / second round trip; 50-50 read-write time; CPU and IO bound
    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      foreman = WorkerRoulette.foreman(sender)
      foreman.enqueue_work_order(work_order)
    end

    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      tradesman = WorkerRoulette.tradesman
      tradesman.work_orders!
    end
  end
end

WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

Benchmark.bmbm do |x|
  x.report "Time for tradesmans to enqueue_work_order and read #{ITERATIONS} large work_orders via pubsub" do # ~1800 work_orders / second round trip
      ITERATIONS.times do |iteration|
        p = -> do
          sender = 'sender_' + iteration.to_s
          foreman = WorkerRoulette.foreman(sender)
          foreman.enqueue_work_order(work_order)
        end
      tradesman = WorkerRoulette.tradesman
      tradesman.wait_for_work_orders(p) {|m| m}
    end
  end
end

WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

# EM.run do
#   EM.add_timer(6) {puts "em off";EM.stop}
#   tradesmans = []
#   foremans = []
#   @start = Time.now
#   @end = nil
#   ITERATIONS.times do |iteration|
#     s = WorkerRoulette.tradesman
#     tradesmans << s
#     sender = 'sender_' + iteration.to_s
#     foreman = WorkerRoulette.foreman(sender)
#     a = -> {foreman.enqueue_work_order(work_order)}
#     s.wait_for_work_orders(a) {|m| @end = Time.now if iteration == (ITERATIONS - 1) }
#   end
# end

# puts  @end - @start
# WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}