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

  context Tradesman do
    let(:foreman) {WorkerRoulette.a_foreman(sender)}
    let(:subject)  {WorkerRoulette.a_tradesman}

    xit "should be working on behalf of a sender" do
      foreman.enqueue_work_order(work_orders) do
        subject.work_orders! do
          subject.sender.should == sender
          done
        end
      end
    end


    xit "should drain one set of work_orders from the sender's slot in the job board" do
      foreman.enqueue_work_order(work_orders) do
        subject.work_orders! do |r|
          r.should == [work_orders_with_headers]
          subject.work_orders! do |r| r.should == []
            subject.work_orders! {|r| r.should == []; done} #does not throw an error if queue is alreay empty
          end
        end
      end
    end

    xit "should take the oldest sender off the job board (FIFO)" do
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

    xit "should get the sender and work_order list transactionally" do
      EM::Hiredis::Client.any_instance.should_receive(:multi).and_call_original
      subject.work_orders! {done}
    end

    xit "should get the work_orders from the next queue when a new job is ready" do
      subject.should_receive(:work_orders!).and_call_original

      subject.wait_for_work_orders do |redis_work_orders, message, channel|
        subject.sender.should == "katie_80"
        redis_work_orders.should == [work_orders_with_headers]
        done
      end

      foreman.enqueue_work_order(work_orders)
    end

    xit "should publish and subscribe on custom channels" do
      good_subscribed   = false
      bad_subscribed    = false

      tradesman         = WorkerRoulette.a_tradesman('good_channel')
      evil_tradesman    = WorkerRoulette.a_tradesman('bad_channel')

      good_foreman      = WorkerRoulette.a_foreman('foreman', 'good_channel')
      bad_foreman       = WorkerRoulette.a_foreman('foreman', 'bad_channel')

      good_publish = ->(a,b) {good_subscribed =  true}
      bad_publish  = ->(a,b) {bad_subscribed =  true}

      tradesman.should_receive(:work_orders!).and_call_original
      evil_tradesman.should_receive(:work_orders!).and_call_original

      #They are double subscribing; is it possible that it is the connection pool?

      tradesman.wait_for_work_orders(good_publish) do |good_work|
        good_work.to_s.should match("old fashion")
        good_work.to_s.should_not match("evil")
        good_subscribed.should == true
      end

      evil_tradesman.wait_for_work_orders(bad_publish) do |bad_work|
        bad_work.to_s.should_not match("old fashion")
        bad_work.to_s.should match("evil")
        bad_subscribed.should == true
      end

      good_foreman.enqueue_work_order('some old fashion work')
      bad_foreman.enqueue_work_order('evil biddings you should not carry out')
      done(0.2)
    end

    xit "should unsubscribe from the job board" do
      subject.wait_for_work_orders do |redis_work_orders, message, channel|
        subject.unsubscribe {done}
      end
      EM::Hiredis::PubsubClient.any_instance.should_receive(:close_connection).and_call_original
      foreman.enqueue_work_order(work_orders)
    end

    it "should periodically (random time between 10  and 15 seconds?) poll the job board for new work, in case it missed a notification"
    it "should not delete the messages from the queue until they have been processed succcesfully"
    it "should not cache the sender our counter keys"
    it "should pull off work orders for more than one sender"
  end

  context "Read Lock" do
    it "should checkout a readlock for a queue and put it back when its done processing; lock should expire after 5 minutes?"
    it "should retry doing work on a queue 3 times if it is locked (ex backoff)"
  end

  context "Failure" do
    it "should not put the sender_id and work_orders back if processing fails bc new work_orders may have been processed while that process failed" do; done; end
  end

  context "Concurrent Access" do
    it "should not leak connections"

    xit "should be fork() proof" do
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
