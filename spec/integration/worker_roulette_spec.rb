require "spec_helper"
module WorkerRoulette
  describe WorkerRoulette do
    let(:sender) {'katie_80'}
    let(:work_orders) {["hello", "foreman"]}
    let(:default_headers) {Hash['headers' => {'sender' => sender}]}
    let(:hello_work_order) {Hash['payload' => "hello"]}
    let(:foreman_work_order) {Hash['payload' => "foreman"]}
    let(:work_orders_with_headers) {default_headers.merge({'payload' => work_orders})}
    let(:jsonized_work_orders_with_headers) {[WorkerRoulette.dump(work_orders_with_headers)]}
    let(:worker_roulette) { WorkerRoulette.start }

    let(:redis) {Redis.new(worker_roulette.redis_config)}

    it "should exist" do
      expect(worker_roulette).to be_instance_of(WorkerRoulette)
    end

    context Foreman do
      let(:subject) {worker_roulette.foreman(sender)}

      it "should be working on behalf of a sender" do
        expect(subject.sender).to eq(sender)
      end

      it "should enqueue_work_order two work_orders in the sender's work queue" do
        subject.enqueue_work_order(work_orders.first) {}
        subject.enqueue_work_order(work_orders.last) {}
        expect(redis.lrange(sender, 0, -1)).to eq(work_orders.map {|m| WorkerRoulette.dump(default_headers.merge({'payload' => m})) })
      end

      it "should enqueue_work_order an array of work_orders without headers in the sender's work queue" do
        subject.enqueue_work_order_without_headers(work_orders)
        expect(redis.lrange(sender, 0, -1)).to eq([WorkerRoulette.dump(work_orders)])
      end

      it "should enqueue_work_order an array of work_orders with default headers in the sender's work queue" do
        subject.enqueue_work_order(work_orders)
        expect(redis.lrange(sender, 0, -1)).to eq(jsonized_work_orders_with_headers)
      end

      it "should enqueue_work_order an array of work_orders with additional headers in the sender's work queue" do
        extra_headers = {'foo' => 'bars'}
        subject.enqueue_work_order(work_orders, extra_headers)
        work_orders_with_headers['headers'].merge!(extra_headers)
        expect(redis.lrange(sender, 0, -1)).to eq([WorkerRoulette.dump(work_orders_with_headers)])
      end

      it "should post the sender's id to the job board with an order number" do
        subject.enqueue_work_order(work_orders.first)
        worker_roulette.foreman('other_forman').enqueue_work_order(work_orders.last)
        expect(redis.zrange(subject.job_board_key, 0, -1, with_scores: true)).to eq([[sender, 1.0], ["other_forman", 2.0]])
      end

      it "should generate a monotically increasing score for senders not on the job board, but not for senders already there" do
        other_forman = worker_roulette.foreman('other_forman')
        expect(redis.get(subject.counter_key)).to be_nil
        subject.enqueue_work_order(work_orders.first)
        expect(redis.get(subject.counter_key)).to eq("1")
        subject.enqueue_work_order(work_orders.last)
        expect(redis.get(subject.counter_key)).to eq("1")
        other_forman.enqueue_work_order(work_orders.last)
        expect(redis.get(other_forman.counter_key)).to eq("2")
      end
    end

    context Tradesman do
      let(:foreman) {worker_roulette.foreman(sender)}
      let(:subject)  {worker_roulette.tradesman}

      before do
        foreman.enqueue_work_order(work_orders)
      end

      context 'removing locks from queues' do
        it "for the last_sender's queue" do
          most_recent_sender = 'most_recent_sender'
          most_recent_foreman = worker_roulette.foreman(most_recent_sender)
          most_recent_foreman.enqueue_work_order(work_orders)
          expect(redis.keys("L*:*").length).to eq(0)
          subject.work_orders!
          expect(redis.get("L*:katie_80")).to eq("1")
          expect(redis.keys("L*:*").length).to eq(1)
          subject.work_orders!
          expect(redis.keys("L*:*").length).to eq(1)
          expect(redis.get("L*:most_recent_sender")).to eq("1")
          subject.work_orders!
          expect(redis.keys("L*:*").length).to eq(0)
        end
      end

      it "should have a last sender if it found messages" do
        expect(subject.work_orders!.length).to eq(1)
        expect(subject.last_sender).to eq(sender)
      end

      it "should not have a last sender if it found no messages" do
        expect(subject.work_orders!.length).to eq(1)
        expect(subject.work_orders!.length).to eq(0)
        expect(subject.last_sender).to be_nil
      end

      it "should drain one set of work_orders from the sender's work queue" do
        expect(subject.work_orders!).to eq([work_orders_with_headers])
        expect(subject.work_orders!).to be_empty
        expect(subject.work_orders!).to be_empty #does not throw an error if queue is already empty
      end

      it "should drain all the work_orders from the sender's work queue" do
        foreman.enqueue_work_order(work_orders)
        expect(subject.work_orders!).to eq([work_orders_with_headers, work_orders_with_headers])
        expect(subject.work_orders!).to be_empty
        expect(subject.work_orders!).to be_empty #does not throw an error if queue is already empty
      end

      it "should take the oldest sender off the job board (FIFO)" do
        oldest_sender = sender.to_s
        most_recent_sender = 'most_recent_sender'
        most_recent_foreman = worker_roulette.foreman(most_recent_sender)
        most_recent_foreman.enqueue_work_order(work_orders)
        expect(redis.zrange(subject.job_board_key, 0, -1)).to eq([oldest_sender, most_recent_sender])
        subject.work_orders!
        expect(redis.zrange(subject.job_board_key, 0, -1)).to eq([most_recent_sender])
      end

      it "should get the work_orders from the next queue when a new job is ready, then poll for new work" do
        subject.work_orders!
        expect(subject).to receive(:work_orders!).and_call_original
        expect(subject.timer).to receive(:after)

        foreman.enqueue_work_order(work_orders)

        subject.wait_for_work_orders do |redis_work_orders|
          expect(redis_work_orders).to eq([work_orders_with_headers])
          expect(subject.last_sender).to eq('katie_80')
        end
      end

      it "should publish and subscribe on custom channels" do
        tradesman         = worker_roulette.tradesman('good_channel')
        expect(tradesman).to receive(:work_orders!).and_call_original

        good_foreman      = worker_roulette.foreman('foreman', 'good_channel')
        bad_foreman       = worker_roulette.foreman('foreman', 'bad_channel')

        good_foreman.enqueue_work_order('some old fashion work')
        bad_foreman.enqueue_work_order('evil biddings you should not carry out')

        tradesman.wait_for_work_orders do |work|
          expect(work.to_s).to match("some old fashion work")
          expect(work.to_s).not_to match("evil")
          expect(tradesman.last_sender).to eq('foreman')
        end
      end

      context "Failure" do
        it "should not put the sender_id and work_orders back if processing fails bc new work_orders may have been processed while that process failed" do; end
      end

      context "Concurrent Access" do
        it "should pool its connections" do
          Array.new(100) do
            Thread.new {worker_roulette.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}}
          end.each(&:join)
          worker_roulette.tradesman_connection_pool.with do |pooled_redis|
            expect(pooled_redis.info["connected_clients"].to_i).to be > (worker_roulette.pool_size)
          end
        end

        #This may be fixed soon (10 Feb 2014 - https://github.com/redis/redis-rb/pull/389 and https://github.com/redis/redis-rb/issues/364)
        it "should not be fork() proof -- forking reconnects need to be handled in the calling code (until redis gem is udpated, then we should be fork-proof)" do
          instance = WorkerRoulette.start
          instance.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}
          fork do
            expect {instance.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}}.to raise_error(Redis::InheritedError)
          end
        end
      end
    end
  end
end
