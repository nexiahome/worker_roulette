module QueueMetricTracker
  def tracker_send(msg)
    UDPSocket.new.send(msg, 0, config[:metric_host][:host_ip], config[:metric_host][:host_port])
  end

  def granularity
    config[:granularity] || 1
  end

  def calculate_stats(stat_name, value)
    calculator(stat_name).add(value)
  end

  def calculator(stat_name)
    QueueMetricTracker.calculators[stat_name] ||= QueueMetricTracker::StatCalculator.new(granularity)
  end

  def channel(sender)
    (sender.split ":").first
  end

  def config
    QueueMetricTracker.config
  end

  def message(label, channel, value)
    "#{label},server_name=#{config[:server_name]},channel=#{channel} value=#{value} #{(Time.now.to_f * 1_000_000_000).to_i}"
  end

  def enabled?
    return false if config.empty? || config[:metrics].empty?
    puts "enabled?: #{config.inspect}"

    klass = self.class.to_s.split("::").last.underscore.to_sym
    config[:metrics].first[klass] rescue false
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
        },
        metrics: [options[:metrics]]
      }
    end

    def included(tracker)
      @trackers ||= []
      @trackers << tracker
    end

    def track_all(options)
      @trackers.each do |tracker_class|
        tracker = tracker_class.new
        tracker.track(*options) if tracker.respond_to?(:track)
      end
    end

    def ip_address(server_name)
      server_name == "localhost" ? "127.0.0.1" : Resolv.new.getaddress(server_name).to_s
    end

  end
end
