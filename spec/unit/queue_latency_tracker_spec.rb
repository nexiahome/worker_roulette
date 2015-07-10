require "spec_helper"

module QueueLatencyTracker
  describe ".configure" do
    let(:source_config) { { logstash_server_ip: ip, logstash_port: port, server_name: server_name } }
    let(:ip)            { "1.2.3.4" }
    let(:port)          { 123 }
    let(:server_name)   { "server.example" }


    it "stores the configuration" do
      QueueLatencyTracker.configure(source_config)

      expect(QueueLatencyTracker.config).to eq({
        logstash: {
          server_ip: ip,
          port: port },
        server_name: server_name
      })
    end
  end

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
      let(:expected_json)   { %({"server_name":"#{server_name}","queue_latency (ms)":#{latency * 1000},"channel":"#{channel}"}) }
      let(:ip)              { "1.2.3.4" }
      let(:port)            { 123 }
      let(:latency)         { 123.432 }
      let(:server_name)     { "server.example" }
      let(:channel)         { "a_channel" }
      let(:headers)         { { "queued_at" => queued_at } }
      let(:raw_work_order)  { { "headers" => headers, "payload" => "aPayload" } }
      let(:logstash_config) { { server_ip: ip, port: port } }
      let(:config)          { { logstash: logstash_config, server_name: server_name } }

      before { allow(QueueLatencyTracker).to receive(:config).and_return(config) }
      before { allow(Time).to receive(:now).and_return(queued_at / GRANULARITY + latency) }
      before { allow_any_instance_of(UDPSocket).to receive(:send) }

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
