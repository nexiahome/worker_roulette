module QueueLatencyTracker
  GRANULARITY = 1_000_000

  class Foreman
    def process(work_order, _channel)
      work_order['headers'].merge!(
        "queued_at" => (Time.now.to_f * GRANULARITY).to_i) if work_order.is_a?(Hash) && work_order["headers"]
      work_order
    end
  end

  class Tradesman
    def process(work_order, channel)
      send_latency(work_order["headers"]["queued_at"], channel)
      work_order
    end

    def send_latency(queued_at, channel)
      latency_ns = (Time.now.to_f * GRANULARITY).to_i - queued_at
      logstash_send(latency_json(latency_ns / 1000.0, channel))
    end

    def logstash_send(json)
      UDPSocket.new.send(json, 0, config[:logstash][:server_ip], config[:logstash][:port])
    end

    def latency_json(latency_ms, channel)
      %({"server_name":"#{config[:server_name]}","queue_latency (ms)":#{latency_ms},"channel":"#{channel}"})
    end

    def config
      QueueLatencyTracker.config
    end
  end

  class << self
    attr_reader :config
    def configure(config)
      @config = {
        logstash: {
          server_ip: config[:logstash_server_ip],
          port: config[:logstash_port] },
        server_name: config[:server_name]
      }
    end

  end
end
