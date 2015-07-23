require_relative "queue_metric_tracker"

module WorkerRoulette
  class QueueDepth
    include ::QueueMetricTracker
    attr_reader :channel

    def initialize(channel)
      @channel = channel
      @queue_depth_sum = @queue_depth_count = 0
    end

    def monitor(_sender, work_orders, remaining)
      batch_size = work_orders.length
      return if batch_size == 0

      monitor_queue_depth(remaining)
      monitor_batch_size(batch_size)
    end

    def monitor_queue_depth(size)
      if value = calculate_stats(:queue_depth, size)
        tracker_send(monitor_json("queue_depth", channel, value))
      end
    end

    def monitor_batch_size(size)
      if value = calculate_stats(:batch_size, size)
        tracker_send(monitor_json("batch_size", channel, value))
      end
    end
  end
end
