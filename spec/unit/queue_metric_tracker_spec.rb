require "spec_helper"

module QueueMetricTracker
  describe ".configure" do
    let(:source_config) { { metric_host: host, metric_host_port: port, server_name: server_name } }
    let(:host)          { "a_metric_host" }
    let(:port)          { 123 }
    let(:ip)            { "1.2.3.4" }
    let(:server_name)   { "server.example" }


    it "stores the configuration" do
      allow(QueueMetricTracker).to receive(:ip_address).and_return(ip)
      QueueMetricTracker.configure(source_config)

      expect(QueueMetricTracker.config).to eq({
        metric_host: {
          host_ip:   ip,
          host_port: port },
        server_name: server_name
      })
    end
  end
end
