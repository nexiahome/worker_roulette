require 'spec_helper'
module WorkerRoulette
 describe "Read Lock" do
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

    before do
      WorkerRoulette.start(evented: false)
      Lua.clear_cache!
      redis.script(:flush)
      redis.flushdb
      foreman.enqueue_work_order(work_orders)
      subject.work_orders!.should == [work_orders_with_headers]
    end

    it "should lock a queue when it reads from it" do
      redis.get(lock_key).should_not be_nil
    end

    it "should set the lock to expire in 1 second" do
      redis.ttl(lock_key).should == 1
    end

    it "should not read a locked queue" do
      foreman.enqueue_work_order(work_orders)    #locked
      subject_two.work_orders!.should == []
    end

    it "should read from the first available queue that is not locked" do
       foreman.enqueue_work_order(work_orders)     #locked
       number_two.enqueue_work_order(work_orders)  #unlocked
       subject_two.work_orders!.first['headers']['sender'].should == 'number_two'
    end

    it "should release its previous lock when it asks for work from another sender" do
      number_two.enqueue_work_order(work_orders)    #unlocked
      subject.last_sender.should == sender
      subject.work_orders!.first['headers']['sender'].should == 'number_two'
      redis.get(lock_key).should == nil
    end

    it "should not release its lock when it asks for work from the same sender" do
      foreman.enqueue_work_order(work_orders)    #locked
      subject.work_orders!.should == [work_orders_with_headers]
      subject.last_sender.should == sender

      foreman.enqueue_work_order(work_orders)    #locked
      subject.work_orders!.should == [work_orders_with_headers]
      subject.last_sender.should == sender

      redis.get(lock_key).should_not == nil
    end

    it "should release its previous lock if there is no work to do from the same sender" do
      foreman.enqueue_work_order(work_orders)    #locked
      subject.work_orders!.should == [work_orders_with_headers]
      subject.work_orders!.should == []
      redis.get(lock_key).should == nil
    end

    xit "pubsub should clean up one contention orremove the lock on the same sender queue automaticly" do

    end
  end
end