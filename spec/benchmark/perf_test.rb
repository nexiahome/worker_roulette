require_relative '../spec_helper'
require 'benchmark'

ITERATIONS = 100_000

Switchboard.start(100)
message = ["hello", 'operator']
namespace = 'switchboard_perf_test'
Benchmark.bmbm do |x|
  x.report "Time to do #{ITERATIONS} inserts from #{ITERATIONS} operators" do
    ITERATIONS.times do |iteration|
      sender = 'sender_' + iteration.to_s
      operator = Switchboard.operator(namespace, sender)
      operator.enqueue(message)
    end
  end
end

Switchboard.pooled_redis_client.flushdb