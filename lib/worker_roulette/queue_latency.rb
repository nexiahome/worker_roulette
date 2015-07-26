module WorkerRoulette
  module QueueLatency
    GRANULARITY = 1_000_000

    class Foreman
      def process(work_order, _channel)
        work_order['headers'].merge!(
          "queued_at" => (Time.now.to_f * GRANULARITY).to_i) if work_order.is_a?(Hash) && work_order["headers"]
        work_order
      end
    end

    class Tradesman
      include QueueMetricTracker

      def process(work_order, channel)
        send_latency(work_order["headers"]["queued_at"], channel)
        work_order
      end

      def send_latency(queued_at, channel)
        return unless queued_at

        latency_ns = (Time.now.to_f * GRANULARITY).to_i - queued_at

        if value = calculate_stats(:queue_latency, latency_ns / 1000.0)
          tracker_send(message("queue_latency(ms)", channel, value))
        end
      end
    end
  end
end
