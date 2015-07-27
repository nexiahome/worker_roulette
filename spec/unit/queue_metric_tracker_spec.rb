require "spec_helper"

module QueueMetricTracker
  describe QueueMetricTracker do
    let(:host)          { "localhost" }
    let(:port)          { 123 }
    let(:ip)            { "1.2.3.4" }
    let(:server_name)   { "server.example" }
    let(:source_config) { { metric_host: host, metric_host_port: port, server_name: server_name } }

    describe ".configure" do
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

    describe "#enabled?" do
      let(:config) { {} }
      subject(:metric) { WorkerRoulette::BatchSize }
      subject(:metric_object) { metric.new }

      before { QueueMetricTracker.configure(source_config.merge(config)) }

      context "when the config is nil" do
        it "returns false" do
          expect(metric_object.enabled?).to be_falsey
        end
      end

      context "when the config has no metrics defined" do
        let(:config) { { metrics: {}} }

        it "returns false" do
          expect(metric_object.enabled?).to be_falsey
        end
      end

      context "when the metric is false" do
        let(:config) { { metrics: { "batch_size" => false } } }

        it "returns false" do
          expect(metric_object.enabled?).to be_falsey
        end
      end

      context "when the metric is true" do
        let(:config) { { metrics: { "batch_size" => true }} }

        it "returns true" do
          expect(metric_object.enabled?).to be_truthy
        end
      end
    end
  end
end
