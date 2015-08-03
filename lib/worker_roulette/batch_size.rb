require_relative "queue_metric_tracker"

module WorkerRoulette
  class BatchSize
    include ::QueueMetricTracker

    def track(sender, work_orders, _remaining)
      return unless enabled?

      batch_size = work_orders.length
      return if batch_size == 0

      if value = calculate_stats(:batch_size, batch_size)
        tracker_send(message("batch_size", channel(sender), value))
      end
    end
  end
end
