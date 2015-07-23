module QueueMetricTracker
  def tracker_send(json)
    UDPSocket.new.send(json, 0, config[:metric_host][:host_ip], config[:metric_host][:host_port])
  end

  def granularity
    config[:granularity] || 5
  end

  def calculate_stats(stat_name, value)
    calculator(stat_name).add(value)
  end

  def calculator(stat_name)
    QueueMetricTracker.calculators[stat_name] ||= QueueMetricTracker::StatCalculator.new(granularity)
  end

  def config
    QueueMetricTracker.config
  end

  def monitor_json(label, channel, value)
    %({"server_name":"#{config[:server_name]}","#{label}":#{value},"channel":"#{channel}"})
  end

  class << self
    attr_reader :config, :calculators
    def configure(options)
      @calculators = {}
      @config = {
        server_name: options[:server_name],
        metric_host: {
          host_ip:   ip_address(options[:metric_host]),
          host_port: options[:metric_host_port]
        }
      }
    end

    def ip_address(server_name)
      server_name == "localhost" ? "127.0.0.1" : Resolv.new.getaddress(server_name).to_s
    end

  end
end
