require_relative '../spec_helper'
require 'benchmark'
require 'eventmachine'

REDIS_CONNECTION_POOL_SIZE = 100
ITERATIONS = 100_000

work_order = {'ding dong' => "hello_foreman_" * 100}

# WorkerRoulette.start(size: REDIS_CONNECTION_POOL_SIZE, evented: true)#{driver: :synchrony}
# WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

# puts "Redis Connection Pool Size: #{REDIS_CONNECTION_POOL_SIZE}"

# Benchmark.bmbm do |x|
#   x.report "Time to insert and read #{ITERATIONS} large work_orders" do # ~2500 work_orders / second round trip; 50-50 read-write time; CPU and IO bound
#     ITERATIONS.times do |iteration|
#       sender = 'sender_' + iteration.to_s
#       foreman = WorkerRoulette.foreman(sender)
#       foreman.enqueue_work_order(work_order)
#     end

#     ITERATIONS.times do |iteration|
#       sender = 'sender_' + iteration.to_s
#       tradesman = WorkerRoulette.tradesman
#       tradesman.work_orders!
#     end
#   end
# end

EM::Hiredis.reconnect_timeout = 0.01

Benchmark.bmbm do |x|
  x.report "Time to evently insert and read #{ITERATIONS} large work_orders" do # ~2500 work_orders / second round trip; 50-50 read-write time; CPU and IO bound
    EM.run do

      WorkerRoulette.start(evented: true)#{driver: :synchrony}
      WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}
      @total = 0
      @tradesman = WorkerRoulette.a_tradesman

      ITERATIONS.times do |iteration|
        sender = 'sender_' + iteration.to_s
        foreman = WorkerRoulette.a_foreman(sender)
        foreman.enqueue_work_order(work_order) do
          @tradesman.work_orders! do
            @total += 1
            EM.stop if @total == (ITERATIONS - 1)
          end
        end
      end
    end
  end
end

# Benchmark.bmbm do |x|
#   x.report "Time to evently pubsub insert and read #{ITERATIONS} large work_orders" do # ~2500 work_orders / second round trip; 50-50 read-write time; CPU and IO bound
#     EM.run do
#       @processed = 0
#       @total     = 0
#       WorkerRoulette.start(evented: true)#{driver: :synchrony}
#       WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}
#       @total = 0
#       @tradesman = WorkerRoulette.a_tradesman
#       on_subscribe = ->(*args) do
#         ITERATIONS.times do |iteration|
#           sender = 'sender_' + iteration.to_s
#           foreman = WorkerRoulette.a_foreman(sender)
#           foreman.enqueue_work_order(work_order)
#         end
#       end
#       @tradesman.wait_for_work_orders(on_subscribe) {@processed += 1; EM.stop if @processed == (ITERATIONS - 1)}
#     end
#   end
# end

# WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

# Benchmark.bmbm do |x|
#   x.report "Time for tradesmans to enqueue_work_order and read #{ITERATIONS} large work_orders via pubsub" do # ~1800 work_orders / second round trip
#     ITERATIONS.times do |iteration|
#       p = -> do
#         sender = 'sender_' + iteration.to_s
#         foreman = WorkerRoulette.foreman(sender)
#         foreman.enqueue_work_order(work_order)
#       end
#       tradesman = WorkerRoulette.tradesman
#       tradesman.wait_for_work_orders(p) {|m| m; tradesman.unsubscribe}
#     end
#   end
# end

# WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

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
