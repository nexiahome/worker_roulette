require_relative "queue_metric_tracker"

module WorkerRoulette
  class QueueDepth
    include ::QueueMetricTracker

    def track(sender, work_orders, remaining)
      return unless enabled?

      if value = calculate_stats(:queue_depth, remaining)
        tracker_send(message("queue_depth", channel(sender), value))
      end
    end
  end
end
