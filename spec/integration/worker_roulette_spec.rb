require "spec_helper"

describe WorkerRoulette do
  let(:sender) {'katie_80'}
  let(:work_orders) {["hello", "foreman"]}
  let(:default_headers) {Hash['headers' => {'sender' => sender}]}
  let(:hello_work_order) {Hash['payload' => "hello"]}
  let(:foreman_work_order) {Hash['payload' => "foreman"]}
  let(:work_orders_with_headers) {default_headers.merge({'payload' => work_orders})}
  let(:jsonized_work_orders_with_headers) {[Oj.dump(work_orders_with_headers)]}

  let(:redis) {Redis.new(WorkerRoulette.redis_config)}

  before do
    WorkerRoulette.start
  end

  it "should exist" do
    WorkerRoulette.should_not be_nil
  end

  context Foreman do
    let(:subject) {WorkerRoulette.foreman(sender)}

    it "should be working on behalf of a sender" do
      subject.sender.should == sender
    end

    it "should be injected with a raw_redis_client so it can do is work" do
      Redis.any_instance.should_receive(:rpush)
      subject.enqueue_work_order(:whatever)
    end

    it "should enqueue_work_order two work_orders in the sender's slot in the switchboard" do
      subject.enqueue_work_order(work_orders.first)
      subject.enqueue_work_order(work_orders.last)
      redis.lrange(sender, 0, -1).should == work_orders.map {|m| Oj.dump(default_headers.merge({'payload' => m})) }
    end

    it "should enqueue_work_order an array of work_orders without headers in the sender's slot in the switchboard" do
      subject.enqueue_work_order_without_headers(work_orders)
      redis.lrange(sender, 0, -1).should == [Oj.dump(work_orders)]
    end

    it "should enqueue_work_order an array of work_orders with default headers in the sender's slot in the switchboard" do
      subject.enqueue_work_order(work_orders)
      redis.lrange(sender, 0, -1).should == jsonized_work_orders_with_headers
    end

    it "should enqueue_work_order an array of work_orders with additional headers in the sender's slot in the switchboard" do
      extra_headers = {'foo' => 'bars'}
      subject.enqueue_work_order(work_orders, extra_headers)
      work_orders_with_headers['headers'].merge!(extra_headers)
      redis.lrange(sender, 0, -1).should == [Oj.dump(work_orders_with_headers)]
    end

    it "should post the sender's id to the job board with an order number" do
      subject.enqueue_work_order(work_orders.first)
      subject.enqueue_work_order(work_orders.last)
      redis.zrange(subject.job_board_key, 0, -1, with_scores: true).should == [[sender.to_s, work_orders.length.to_f]]
    end

    it "should post the sender_id and work_orders transactionally" do
      Redis.any_instance.should_receive(:multi)
      subject.enqueue_work_order(work_orders.first)
    end

    it "should generate sequential order numbers" do
      redis.get(subject.counter_key).should == nil
      subject.enqueue_work_order(work_orders.first)
      redis.get(subject.counter_key).should == "1"
      subject.enqueue_work_order(work_orders.last)
      redis.get(subject.counter_key).should == "2"
    end

    it "should publish a notification that a new job is ready" do
      result = nil
      redis_tradesman = Redis.new
      redis_tradesman.subscribe(WorkerRoulette::JOB_NOTIFICATIONS) do |on|
        on.subscribe do |channel, subscription|
          subject.enqueue_work_order(work_orders)
        end

        on.message do |channel, notification|
          result = notification
          redis_tradesman.unsubscribe(WorkerRoulette::JOB_NOTIFICATIONS)
        end
     end

      result.should == WorkerRoulette::JOB_NOTIFICATIONS
    end
  end

  context Tradesman do
    let(:foreman) {WorkerRoulette.foreman(sender)}
    let(:subject)  {WorkerRoulette.tradesman}

    before do
      foreman.enqueue_work_order(work_orders)
    end

    it "should be working on behalf of a sender" do
      subject.work_orders!
      subject.sender.should == sender
    end

    it "should be injected with a redis client so it can do its work" do
      Redis.any_instance.should_receive(:lrange).and_call_original
      subject.work_orders!
    end

    it "should drain one set of work_orders from the sender's slot in the switchboard" do
      subject.work_orders!.should == [work_orders_with_headers]
      subject.work_orders!.should == []
      subject.work_orders!.should == [] #does not throw an error if queue is alreay empty
    end

    it "should drain all the work_orders from the sender's slot in the switchboard" do
      foreman.enqueue_work_order(work_orders)
      subject.work_orders!.should == [work_orders_with_headers, work_orders_with_headers]
      subject.work_orders!.should == []
      subject.work_orders!.should == [] #does not throw an error if queue is alreay empty
    end

    it "should take the oldest sender off the job board (FIFO)" do
      oldest_sender = sender.to_s
      most_recent_sender = 'most_recent_sender'
      most_recent_foreman = WorkerRoulette.foreman(most_recent_sender)
      most_recent_foreman.enqueue_work_order(work_orders)
      redis.zrange(subject.job_board_key, 0, -1).should == [oldest_sender, most_recent_sender]
      subject.work_orders!
      redis.zrange(subject.job_board_key, 0, -1).should == [most_recent_sender]
    end

    it "should get the sender and work_order list transactionally" do
      Redis.any_instance.should_receive(:multi).and_call_original
      subject.work_orders!
    end

    it "should get the work_orders from the next sender's slot when a new job is ready" do
      subject.work_orders!
      subject.should_receive(:work_orders!).and_call_original
      publisher = -> {foreman.enqueue_work_order(work_orders); subject.unsubscribe; }
      subject.wait_for_work_orders(publisher) do |redis_work_orders|
        redis_work_orders.should == [work_orders_with_headers]
      end
    end

    it "should checkout a readlock for a queue and put it back when its done processing; lock should expire after 5 minutes?"
    it "should eves drop on the job board"

    context "Failure" do
      it "should not put the sender_id and work_orders back if processing fails bc new work_orders may have been processed while that process failed" do; end
    end

    context "Concurrent Access" do
      it "should pool its connections" do
        Array.new(100) do
          Thread.new {WorkerRoulette.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}}
        end.each(&:join)
        WorkerRoulette.tradesman_connection_pool.with do |pooled_redis|
          pooled_redis.info["connected_clients"].to_i.should > (WorkerRoulette.pool_size)
        end
      end

      #This may be fixed soon (10 Feb 2014 - https://github.com/redis/redis-rb/pull/389 and https://github.com/redis/redis-rb/issues/364)
      it "should not be fork() proof -- forking reconnects need to be handled in the calling code (until redis gem is udpated, then we should be fork-proof)" do
        WorkerRoulette.start
        WorkerRoulette.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}
        fork do
          expect {WorkerRoulette.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}}.to raise_error(Redis::InheritedError)
        end
      end

      it "should use optionally non-blocking I/O" do
         EM.synchrony do
          WorkerRoulette.start(:driver => :synchrony)
          WorkerRoulette.foreman("muddle_man").enqueue_work_order("foo")
          WorkerRoulette.tradesman.work_orders!.should == [work_orders_with_headers]
          EM.stop
        end
      end
    end
  end
end