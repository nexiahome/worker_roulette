require "spec_helper"

module WorkerRoulette::QueueLatency
  describe Foreman do
    describe "#process" do
      let(:channel)        { "a_channel" }
      let(:queued_at)      { 1234567 }
      let(:raw_work_order) { { "headers" => {}, "payload" => "aPayload" } }
      let(:work_order)     { subject.process(raw_work_order, channel) }

      before { allow(Time).to receive(:now).and_return(queued_at) }

      it "sets queued_at to now using specified granularity" do
        expect(work_order["headers"]["queued_at"]).to eq(queued_at * GRANULARITY)

      end
    end
  end

  describe Tradesman do
    describe "#process" do
      let(:queued_at)       { 1234567 * GRANULARITY }
      let(:host)            { "a_metric_host" }
      let(:ip)              { "1.2.3.4" }
      let(:port)            { 123 }
      let(:latency)         { 123.432 }
      let(:server_name)     { "server.example" }
      let(:channel)         { "a_channel" }
      let(:headers)         { { "queued_at" => queued_at } }
      let(:raw_work_order)  { { "headers" => headers, "payload" => "aPayload" } }
      let(:metric_config)   { { host_ip: ip, host_port: port } }
      let(:config)          { { metric_host: metric_config, server_name: server_name } }
      let(:expected_json)   { %({"server_name":"#{server_name}","queue_latency (ms)":#{latency},"channel":"#{channel}"}) }

      before { allow(QueueMetricTracker).to receive(:config).and_return(config) }
      before { allow(Time).to receive(:now).and_return(queued_at / GRANULARITY + latency) }
      before { allow_any_instance_of(UDPSocket).to receive(:send) }
      before { allow_any_instance_of(QueueMetricTracker).to receive(:calculate_stats).and_return(latency) }
      before { allow(QueueMetricTracker).to receive(:ipaddress).and_return(ip) }

      it "passes the right json to logstash_send" do
        expect_any_instance_of(UDPSocket).to receive(:send).with(expected_json, 0, ip, port)

        subject.process(raw_work_order, channel)
      end

      it "returns the work order unchanged" do
        expect(subject.process(raw_work_order, channel)).to eq(raw_work_order)
      end
    end
  end
end
