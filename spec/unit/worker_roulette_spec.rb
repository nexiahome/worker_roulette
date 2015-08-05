require "spec_helper"

module WorkerRoulette
  describe WorkerRoulette do
    DEFAULT_POLLING_TIME = 2
    let(:redis_host) { "redis_host" }
    let(:db) { 7 }
    let(:metric_host_name) { "localhost" }
    let(:metric_port) { 7777 }
    let(:granularity) { 10 }
    let(:server_name) { `hostname`.chomp || "hostname" }

    let(:symbolized_config) { {
      host: redis_host,
      db: db,
      metric_tracker: {
        metric_host: metric_host_name,
        metric_host_port: metric_port,
        granularity: granularity,
        metrics: {
          batch_size: true,
          queue_depth: true,
          queue_latency: true
        }
      }
    } }

    let(:stringified_config) { {
      "host" => redis_host,
      "db"   => 7,
      "metric_tracker"     => {
        "metric_host"      => metric_host_name,
        "metric_host_port" => metric_port,
        "granularity"      => granularity,
        "metrics"          => {
          "batch_size"     => true,
          "queue_depth"    => true,
          "queue_latency"  => true,
        }
      }
    } }

    let(:redis_config) { {
      host: redis_host,
      driver: :hiredis,
      port: 6379,
      db: db
    } }

    let(:metrics_config) { {
      granularity: granularity,
      metric_host: metric_host_name,
      metric_host_port: metric_port,
      metrics: {
        batch_size: true,
        queue_depth: true,
        queue_latency: true
      }
    } }

    let(:metric_host) { {
        host_ip:   "127.0.0.1",
        host_port: metric_port
    } }

    let(:metric_tracker_config) { metrics_config.delete_if{ |k| k == :metric_host_port }.merge(metric_host: metric_host).merge(server_name: server_name) }

    subject(:worker_roulette) { WorkerRoulette.start(options) }

    describe "initialize" do
      context "when config hash has symbol keys" do
        let(:options) { symbolized_config }

        it "successfully interprets the config hash" do
          expect(subject.redis_config.delete_if{ |k| k == :metric_tracker }).to eq(redis_config)
        end

        it "successfully interprets the metrics hash" do
          expect(QueueMetricTracker.config).to eq(metric_tracker_config)
        end
      end

      context "when config hash has string keys" do
        let(:options) { stringified_config }
        it "successfully interprets the config hash" do
          expect(subject.redis_config.delete_if{ |k| k == :metric_tracker }).to eq(redis_config)
        end

        it "successfully interprets the metrics hash" do
          expect(QueueMetricTracker.config).to eq(metric_tracker_config)
        end
      end
    end
  end
end
