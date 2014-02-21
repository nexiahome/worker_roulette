require "spec_helper"

describe WorkerRoulette do
  include EventedSpec::EMSpec

  let(:sender) {'katie_80'}
  let(:work_orders) {["hello", "foreman"]}
  let(:default_headers) {Hash['headers' => {'sender' => sender}]}
  let(:hello_work_order) {Hash['payload' => "hello"]}
  let(:foreman_work_order) {Hash['payload' => "foreman"]}
  let(:work_orders_with_headers) {default_headers.merge({'payload' => work_orders})}
  let(:jsonized_work_orders_with_headers) {[WorkerRoulette.dump(work_orders_with_headers)]}

  let(:redis) {Redis.new(WorkerRoulette.redis_config)}

  em_before do
    WorkerRoulette.start(evented: true)
  end

  context "Evented Foreman" do
    let(:subject) {WorkerRoulette.a_foreman(sender)}

    it "should enqueue work" do
      called = false
      foreman      = WorkerRoulette.a_foreman('foreman')
      foreman.enqueue_work_order('some old fashion work') do |redis_response, stuff|
        called = true
        redis_response.should == 'foreman added'
      end
      done(0.1) {called.should == true}
    end

    it "should enqueue_work_order two work_orders in the sender's slot in the job board" do
      subject.enqueue_work_order(work_orders.first) do
        subject.enqueue_work_order(work_orders.last) do
          redis.lrange(sender, 0, -1).should == work_orders.map {|m| WorkerRoulette.dump(default_headers.merge({'payload' => m})) }
          done
        end
      end
    end

    it "should enqueue_work_order an array of work_orders without headers in the sender's slot in the job board" do
      subject.enqueue_work_order_without_headers(work_orders) do
        redis.lrange(sender, 0, -1).should == [WorkerRoulette.dump(work_orders)]
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
        redis.lrange(sender, 0, -1).should == [WorkerRoulette.dump(work_orders_with_headers)]
        done
      end
    end

    it "should post the sender's id to the job board with an order number" do
      first_foreman      = WorkerRoulette.a_foreman('first_foreman')
      first_foreman.enqueue_work_order('foo') do
        subject.enqueue_work_order(work_orders.first) do
          subject.enqueue_work_order(work_orders.last) do
            redis.zrange(subject.job_board_key, 0, -1, with_scores: true).should == [["first_foreman", 1.0], ["katie_80", 2.0]]
            done
          end
        end
      end
    end

    it "should generate a monotically increasing score for senders not on the job board, but not for senders already there" do
      first_foreman = WorkerRoulette.a_foreman('first_foreman')
      redis.get(subject.counter_key).should == nil
      first_foreman.enqueue_work_order(work_orders.first) do
        redis.get(subject.counter_key).should == "1"
        first_foreman.enqueue_work_order(work_orders.last) do
          redis.get(subject.counter_key).should == "1"
          subject.enqueue_work_order(work_orders.first) do
            redis.get(subject.counter_key).should == "2"
            done
          end
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

  context "Evented Tradesman" do
    let(:foreman) {WorkerRoulette.a_foreman(sender)}
    let(:subject)  {WorkerRoulette.a_tradesman}

    it "should be working on behalf of a sender" do
      foreman.enqueue_work_order(work_orders) do
        subject.work_orders! do |r|
          subject.last_sender.should == sender
          done
        end
      end
    end


    it "should drain one set of work_orders from the sender's slot in the job board" do
      foreman.enqueue_work_order(work_orders) do
        subject.work_orders! do |r|
          r.should == [work_orders_with_headers]
          subject.work_orders! do |r| r.should == []
            subject.work_orders! {|r| r.should == []; done} #does not throw an error if queue is alreay empty
          end
        end
      end
    end

    it "should take the oldest sender off the job board (FIFO)" do
      foreman.enqueue_work_order(work_orders) do
        oldest_sender = sender.to_s
        most_recent_sender = 'most_recent_sender'
        most_recent_foreman = WorkerRoulette.a_foreman(most_recent_sender)
        most_recent_foreman.enqueue_work_order(work_orders) do
          redis.zrange(subject.job_board_key, 0, -1).should == [oldest_sender, most_recent_sender]
          subject.work_orders! { redis.zrange(subject.job_board_key, 0, -1).should == [most_recent_sender]; done }
        end
      end
    end

    it "should get the work_orders from the next queue when a new job is ready" do
      subject.should_receive(:work_orders!).and_call_original
      publish = proc {foreman.enqueue_work_order(work_orders)}

      subject.wait_for_work_orders(publish) do |redis_work_orders, message, channel|
        subject.last_sender.should == "katie_80"
        redis_work_orders.should == [work_orders_with_headers]
        done
      end

    end

    it "should publish and subscribe on custom channels" do
      good_subscribed   = false
      bad_subscribed    = false

      tradesman         = WorkerRoulette.a_tradesman('good_channel')
      evil_tradesman    = WorkerRoulette.a_tradesman('bad_channel')

      good_foreman      = WorkerRoulette.a_foreman('foreman', 'good_channel')
      bad_foreman       = WorkerRoulette.a_foreman('foreman', 'bad_channel')

      good_publish = proc {good_foreman.enqueue_work_order('some old fashion work')}
      bad_publish  = proc {bad_foreman.enqueue_work_order('evil biddings you should not carry out')}

      tradesman.should_receive(:work_orders!).and_call_original
      evil_tradesman.should_receive(:work_orders!).and_call_original

      #They are double subscribing; is it possible that it is the connection pool?

      tradesman.wait_for_work_orders(good_publish) do |good_work|
        good_work.to_s.should match("old fashion")
        good_work.to_s.should_not match("evil")
      end

      evil_tradesman.wait_for_work_orders(bad_publish) do |bad_work|
        bad_work.to_s.should_not match("old fashion")
        bad_work.to_s.should match("evil")
      end

      done(0.2)
    end

    it "should unsubscribe from the job board" do
      publish = proc {foreman.enqueue_work_order(work_orders)}
      subject.wait_for_work_orders(publish) do |redis_work_orders, message, channel|
        subject.unsubscribe {done}
      end
      EM::Hiredis::PubsubClient.any_instance.should_receive(:close_connection).and_call_original
    end

    it "should periodically (random time between 20 and 25 seconds?) poll the job board for new work, in case it missed a notification" do
      EM::PeriodicTimer.should_receive(:new) {|time| time.should be_within(2.5).of(22.5)}
      publish = proc {foreman.enqueue_work_order('foo')}
      subject.wait_for_work_orders(publish) {done}
    end

    xit "should cancel the old timer when the on_message callback is called" do
      publish = proc {foreman.enqueue_work_order('foo')}
      subject.wait_for_work_orders(publish) do
        subject.send(:timer).should_receive(:cancel).and_call_original
        done
      end
    end

    it "should pull off work orders for more than one sender" do
      tradesman         = WorkerRoulette.a_tradesman('good_channel')

      good_foreman      = WorkerRoulette.a_foreman('good_foreman', 'good_channel')
      lazy_foreman      = WorkerRoulette.a_foreman('lazy_foreman', 'good_channel')

      got_good = false
      got_lazy  = false
      good_foreman.enqueue_work_order('do good work') do
        tradesman.work_orders! do |r|
          got_good = true
          r.first['payload'].should == ('do good work')
        end
      end
      lazy_foreman.enqueue_work_order('just get it done') do
        tradesman.work_orders! do |r|
          got_lazy = true
          r.first['payload'].should == ('just get it done')
        end
      end

      done(0.2) {(got_good && got_lazy).should == true}
    end
  end

  xit "should return a hash with a string in the payload if OJ cannot parse the json" do

  end

  context "Failure" do
    it "should not put the sender_id and work_orders back if processing fails bc new work_orders may have been processed while that process failed" do; done; end
  end

  context "Concurrent Access" do
    it "should not leak connections"

    it "should be fork() proof" do
      @subject = WorkerRoulette.a_tradesman
      @subject.work_orders! do
        fork do
          @subject.work_orders!
        end
      end
      done(1)
    end
  end
end
