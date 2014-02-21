require 'worker_roulette'
require 'benchmark'
require 'eventmachine'

REDIS_CONNECTION_POOL_SIZE = 100
ITERATIONS = 10_000

work_order = {'ding dong' => "hello_foreman_" * 100}

EM::Hiredis.reconnect_timeout = 0.01

puts "Redis Connection Pool Size: #{REDIS_CONNECTION_POOL_SIZE}"

times = Benchmark.bm do |x|
  x.report "#{ITERATIONS} ASync Api Read/Writes" do
    EM.run do
      WorkerRoulette.start(evented: true)
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
puts "#{ITERATIONS / times.first.real} ASync Api Read/Writes per second"
puts "#################"
puts

WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

times = Benchmark.bm do |x|
  x.report "#{ITERATIONS * 2} ASync Api Pubsub Read/Writes" do
    EM.run do
      WorkerRoulette.start(evented: true)
      @processed = 0
      @total     = 0
      WorkerRoulette.start(evented: true)
      WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}
      @total = 0
      @tradesman = WorkerRoulette.a_tradesman
      on_subscribe = ->(*args) do
        ITERATIONS.times do |iteration|
          sender = 'sender_' + iteration.to_s
          foreman = WorkerRoulette.a_foreman(sender)
          foreman.enqueue_work_order(work_order)
        end
      end
      @tradesman.wait_for_work_orders(on_subscribe) {@processed += 1; EM.stop if @processed == (ITERATIONS - 1)}
    end
  end
end
puts "#{ITERATIONS * 2 / times.first.real} ASync Api Pubsub Read/Writes per second"
puts "#################"
puts
WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

WorkerRoulette.start(size: REDIS_CONNECTION_POOL_SIZE, evented: false)
times = Benchmark.bm do |x|
  puts x.class.name
  x.report "#{ITERATIONS} Sync Api Writes" do
    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      foreman = WorkerRoulette.foreman(sender)
      foreman.enqueue_work_order(work_order)
    end
  end
  WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}
end
WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

puts "#{ITERATIONS / times.first.real} Sync Api Writes per second"
puts "#################"
puts
ITERATIONS.times do |iteration|
  sender = 'sender_' + iteration.to_s
  foreman = WorkerRoulette.foreman(sender)
  foreman.enqueue_work_order(work_order)
end

times = Benchmark.bm do |x|
  x.report "#{ITERATIONS} Sync Api Reads" do
    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      tradesman = WorkerRoulette.tradesman
      tradesman.work_orders!
    end
  end
end
puts "#{ITERATIONS / times.first.real} Sync Api Reads per second"
puts "#################"
puts
WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

times = Benchmark.bm do |x|
  x.report "#{ITERATIONS} Sync Api Read/Writes" do
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
puts "#{ITERATIONS / times.first.real} Sync Api Read/Writes per second"
puts "#################"
puts
WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}

times = Benchmark.bm do |x|
  x.report "#{ITERATIONS * 2} Sync Api Pubsub Read/Writes" do
    WorkerRoulette.start(size: REDIS_CONNECTION_POOL_SIZE, evented: false)
    ITERATIONS.times do |iteration|
      p = -> do
        sender = 'sender_' + iteration.to_s
        foreman = WorkerRoulette.foreman(sender)
        foreman.enqueue_work_order(work_order)
      end
      tradesman = WorkerRoulette.tradesman
      tradesman.wait_for_work_orders(p) {|m| m; tradesman.unsubscribe}
    end
  end
end
puts "#{ITERATIONS * 2 / times.first.real} Sync Api Pubsub Read/Writes per second"
puts "#################"
puts
WorkerRoulette.tradesman_connection_pool.with {|r| r.flushdb}