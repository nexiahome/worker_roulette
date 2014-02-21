require 'spec_helper'
module WorkerRoulette
 describe "Evented Read Lock" do
    include EventedSpec::EMSpec

    let(:redis) {Redis.new(WorkerRoulette.redis_config)}
    let(:sender) {'katie_80'}
    let(:work_orders) {"hellot"}
    let(:lock_key) {"L*:#{sender}"}
    let(:default_headers) {Hash['headers' => {'sender' => sender}]}
    let(:work_orders_with_headers) {default_headers.merge({'payload' => work_orders})}
    let(:jsonized_work_orders_with_headers) {[WorkerRoulette.dump(work_orders_with_headers)]}
    let(:foreman) {WorkerRoulette.foreman(sender)}
    let(:number_two) {WorkerRoulette.foreman('number_two')}
    let(:subject) {WorkerRoulette.tradesman}
    let(:subject_two) {WorkerRoulette.tradesman}

    em_before do
      WorkerRoulette.start(evented: true)
      Lua.clear_cache!
      redis.script(:flush)
      redis.flushdb
    end

    it "should lock a queue when it reads from it" do
      evented_readlock_preconditions do
        redis.get(lock_key).should_not be_nil
        done
      end
    end

    it "should set the lock to expire in 1 second" do
      evented_readlock_preconditions do
        redis.ttl(lock_key).should == 1
        done
      end
    end

    it "should not read a locked queue" do
      evented_readlock_preconditions do
        foreman.enqueue_work_order(work_orders) do #locked
          subject_two.work_orders! {|work |work.should == []; done}
        end
      end
    end

    it "should read from the first available queue that is not locked" do
       evented_readlock_preconditions do
         foreman.enqueue_work_order(work_orders) do    #locked
           number_two.enqueue_work_order(work_orders) do  #unlocked
            subject_two.work_orders!{|work| work.first['headers']['sender'].should == 'number_two'; done}
          end
         end
       end
    end

    it "should release its last lock when it asks for its next work order from another sender" do
      evented_readlock_preconditions do
        number_two.enqueue_work_order(work_orders) do #unlocked
          subject.last_sender.should == sender
          subject.work_orders! do |work|
            work.first['headers']['sender'].should == 'number_two'
            redis.get(lock_key).should == nil
            done
          end
        end
      end
    end

    it "should not release its lock when it asks for its next work order from the same sender" do
      evented_readlock_preconditions do
        foreman.enqueue_work_order(work_orders) do #locked
          subject.work_orders! do |work|
            work.should == [work_orders_with_headers]
            subject.last_sender.should == sender
            redis.get(lock_key).should_not == nil
            done
          end
        end
      end
    end

    it "should not take out another lock if there is no work to do" do
      evented_readlock_preconditions do
        foreman.enqueue_work_order(work_orders) do #locked
          subject.work_orders! do |work|
            work.should == [work_orders_with_headers]
            subject.work_orders! do |work|
              work.should == []
              redis.get(lock_key).should == nil
              done
            end
          end
        end
      end
    end
  end

  def evented_readlock_preconditions(&spec_block)
    foreman.enqueue_work_order(work_orders) do
      subject.work_orders! do |work|
        work.should == [work_orders_with_headers]
        spec_block.call
      end
    end
  end
end