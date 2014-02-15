require "spec_helper"

describe WorkerRoulette do
  include EventedSpec::EMSpec

  let(:sender) {'katie_80'}
  let(:work_orders) {["hello", "foreman"]}
  let(:default_headers) {Hash['headers' => {'sender' => sender}]}
  let(:hello_work_order) {Hash['payload' => "hello"]}
  let(:foreman_work_order) {Hash['payload' => "foreman"]}
  let(:work_orders_with_headers) {default_headers.merge({'payload' => work_orders})}
  let(:jsonized_work_orders_with_headers) {[Oj.dump(work_orders_with_headers)]}

  let(:redis) {Redis.new(WorkerRoulette.redis_config)}

  em_before do
    WorkerRoulette.start(evented: true)
  end

  context Foreman do
    let(:subject) {WorkerRoulette.a_foreman(sender)}

    it "should enqueue_work_order two work_orders in the sender's slot in the job board" do
      subject.enqueue_work_order(work_orders.first) do
        subject.enqueue_work_order(work_orders.last) do
          redis.lrange(sender, 0, -1).should == work_orders.map {|m| Oj.dump(default_headers.merge({'payload' => m})) }
          done
        end
      end
    end

    it "should enqueue_work_order an array of work_orders without headers in the sender's slot in the job board" do
      subject.enqueue_work_order_without_headers(work_orders) do
        redis.lrange(sender, 0, -1).should == [Oj.dump(work_orders)]
        done
      end
    end

    it "should enqueue_work_order an array of work_orders with default headers in the sender's slot in the job board" do
      subject.enqueue_work_order(work_orders) do
        redis.lrange(sender, 0, -1).should == jsonized_work_orders_with_headers
        done
      end
    end

    it "should enqueue_work_order an array of work_orders with additional headers in the sender's slot in the job board" do
      extra_headers = {'foo' => 'bars'}
      subject.enqueue_work_order(work_orders, extra_headers) do
        work_orders_with_headers['headers'].merge!(extra_headers)
        redis.lrange(sender, 0, -1).should == [Oj.dump(work_orders_with_headers)]
        done
      end
    end

    it "should post the sender's id to the job board with an order number" do
      subject.enqueue_work_order(work_orders.first) do
        subject.enqueue_work_order(work_orders.last) do
          redis.zrange(subject.job_board_key, 0, -1, with_scores: true).should == [[sender.to_s, work_orders.length.to_f]]
          done
        end
      end
    end

    it "should post the sender_id and work_orders transactionally" do
      EM::Hiredis::Client.any_instance.should_receive(:multi).and_call_original
      subject.enqueue_work_order(work_orders.first)  do
        done
      end
    end

    it "should generate sequential order numbers" do
      redis.get(subject.counter_key).should == nil
      subject.enqueue_work_order(work_orders.first) do
        redis.get(subject.counter_key).should == "1"
        subject.enqueue_work_order(work_orders.last) do
          redis.get(subject.counter_key).should == "2"
          done
        end
      end
    end

    it "should publish a notification that a new job is ready" do
      result = nil
      subscriber = WorkerRoulette.new_redis_pubsub
      subscriber.subscribe(WorkerRoulette::JOB_NOTIFICATIONS) do |message|
        subscriber.unsubscribe(WorkerRoulette::JOB_NOTIFICATIONS)
        message.should == WorkerRoulette::JOB_NOTIFICATIONS
        done
      end.callback { subject.enqueue_work_order(work_orders) }
    end
  end

  # context Tradesman do
  #   let(:foreman) {WorkerRoulette.foreman(sender)}
  #   let(:subject)  {WorkerRoulette.tradesman}

  #   before do
  #     foreman.enqueue_work_order(work_orders)
  #   end

  #   it "should be working on behalf of a sender" do
  #     subject.work_orders!
  #     subject.sender.should == sender
  #   end

  #   it "should be injected with a redis client so it can do its work" do
  #     Redis.any_instance.should_receive(:lrange).and_call_original
  #     subject.work_orders!
  #   end

  #   it "should drain one set of work_orders from the sender's slot in the job board" do
  #     subject.work_orders!.should == [work_orders_with_headers]
  #     subject.work_orders!.should == []
  #     subject.work_orders!.should == [] #does not throw an error if queue is alreay empty
  #   end

  #   it "should drain all the work_orders from the sender's slot in the job board" do
  #     foreman.enqueue_work_order(work_orders)
  #     subject.work_orders!.should == [work_orders_with_headers, work_orders_with_headers]
  #     subject.work_orders!.should == []
  #     subject.work_orders!.should == [] #does not throw an error if queue is alreay empty
  #   end

  #   it "should take the oldest sender off the job board (FIFO)" do
  #     oldest_sender = sender.to_s
  #     most_recent_sender = 'most_recent_sender'
  #     most_recent_foreman = WorkerRoulette.foreman(most_recent_sender)
  #     most_recent_foreman.enqueue_work_order(work_orders)
  #     redis.zrange(subject.job_board_key, 0, -1).should == [oldest_sender, most_recent_sender]
  #     subject.work_orders!
  #     redis.zrange(subject.job_board_key, 0, -1).should == [most_recent_sender]
  #   end

  #   it "should get the sender and work_order list transactionally" do
  #     Redis.any_instance.should_receive(:multi).and_call_original
  #     subject.work_orders!
  #   end

  #   it "should get the work_orders from the next queue when a new job is ready" do
  #     subject.work_orders!
  #     subject.should_receive(:work_orders!).and_call_original

  #     publisher = -> {foreman.enqueue_work_order(work_orders); subject.unsubscribe}

  #     subject.wait_for_work_orders(publisher) do |redis_work_orders|
  #       redis_work_orders.should == [work_orders_with_headers]
  #     end
  #   end

  #   it "should publish and subscribe on custom channels" do
  #     tradesman         = WorkerRoulette.tradesman('good_channel')
  #     tradesman.should_receive(:work_orders!).and_call_original

  #     good_foreman      = WorkerRoulette.foreman('foreman', 'good_channel')
  #     bad_foreman       = WorkerRoulette.foreman('foreman', 'bad_channel')


  #     publish  = -> do
  #       good_foreman.enqueue_work_order('some old fashion work')
  #       bad_foreman.enqueue_work_order('evil biddings you should not carry out')
  #       tradesman.unsubscribe
  #     end

  #     tradesman.wait_for_work_orders(publish) do |work|
  #       work.to_s.should match("some old fashion work")
  #       work.to_s.should_not match("evil")
  #     end
  #   end

  #   it "should checkout a readlock for a queue and put it back when its done processing; lock should expire after 5 minutes?"
  #   it "should eves drop on the job board"

  #   context "Failure" do
  #     it "should not put the sender_id and work_orders back if processing fails bc new work_orders may have been processed while that process failed" do; end
  #   end

  #   context "Concurrent Access" do
  #     it "should pool its connections" do
  #       Array.new(100) do
  #         Thread.new {WorkerRoulette.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}}
  #       end.each(&:join)
  #       WorkerRoulette.tradesman_connection_pool.with do |pooled_redis|
  #         pooled_redis.info["connected_clients"].to_i.should > (WorkerRoulette.pool_size)
  #       end
  #     end

  #     #This may be fixed soon (10 Feb 2014 - https://github.com/redis/redis-rb/pull/389 and https://github.com/redis/redis-rb/issues/364)
  #     it "should not be fork() proof -- forking reconnects need to be handled in the calling code (until redis gem is udpated, then we should be fork-proof)" do
  #       WorkerRoulette.start
  #       WorkerRoulette.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}
  #       fork do
  #         expect {WorkerRoulette.tradesman_connection_pool.with {|pooled_redis| pooled_redis.get("foo")}}.to raise_error(Redis::InheritedError)
  #       end
  #     end

  it "should enqueue work with headers subscribe" do
    called = false
    foreman      = WorkerRoulette.a_foreman('foreman')
    foreman.enqueue_work_order('some old fashion work') do |redis_response|
      called = true
      redis_response.should == [1, 1, 0]
    end
    done(0.1) {called.should == true}
  end
end
