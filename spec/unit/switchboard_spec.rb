require "spec_helper"

describe Switchboard do
  let(:sender) {:katie_80}
  let(:namespace) {:sixties_telephony}
  let(:messages) {["hello", "operator"]}
  let(:raw_redis_client) {Redis.new}
  let(:redis) {Redis::Namespace.new(namespace, redis: raw_redis_client)}

  before {redis.flushdb}

  it "should exist" do
    Switchboard.should_not be_nil
  end

  context Operator do
    let(:subject) {Operator.new(namespace, sender, raw_redis_client)}

    it "should be working on behalf of a sender" do
      subject.sender.should == sender
    end

    it "should be scoped to a namespace" do
      subject.namespace.should == namespace
    end

    it "should be injected with a raw_redis_client so it can do is work" do
      raw_redis_client.should_receive(:rpush)
      subject.enqueue(:whatever)
    end

    it "should enqueue two messages in the sender's slot in the switchboard" do
      subject.enqueue(messages.first)
      subject.enqueue(messages.last)
      redis.lrange(sender, 0, -1).should == messages
    end

    it "should enqueue an array of messages in the sender's slot in the switchboard" do
      subject.enqueue(messages)
      redis.lrange(sender, 0, -1).should == messages
    end

    it "should post the sender's id to the job board with an order number" do
      subject.enqueue(messages.first)
      subject.enqueue(messages.last)
      redis.zrange(subject.job_board_key, 0, -1, with_scores: true).should == [[sender.to_s, messages.length.to_f]]
    end

    it "should post the sender_id and messages transactionally" do
      raw_redis_client.should_receive(:multi)
      subject.enqueue(messages.first)
    end

    it "should generate sequential order numbers" do
      redis.get(subject.counter_key).should == nil
      subject.enqueue(messages.first)
      redis.get(subject.counter_key).should == "1"
    end
  end

  context Subscriber do
    let(:operator) {Operator.new(namespace, sender, raw_redis_client)}
    let(:subject)  {Subscriber.new(namespace, raw_redis_client)}

    before do
      operator.enqueue(messages.first)
      operator.enqueue(messages.last)
    end

    it "should be working on behalf of a sender" do
      subject.messages!
      subject.sender.should == sender
    end

    it "should be scoped to a namespace" do
      subject.namespace.should == namespace
    end

    it "should be injected with a raw_redis_client so it can do is work" do
      raw_redis_client.should_receive(:lrange)
      subject.messages!
    end

    it "should drain all the messages from the sender's slot in the switchboard" do
      subject.messages!.should == messages
      subject.messages!.should == []
      subject.messages!.should == [] #does not throw an error if queue is alreay empty
    end

    it "should take the most recent sender_id off the job board" do
      redis.zrange(subject.job_board_key, 0, -1).should == [sender.to_s]
      subject.messages!
      redis.zrange(subject.job_board_key, 0, -1).should == []
    end

    it "should get the sender_id and message list transactionally" do
      raw_redis_client.should_receive(:multi).and_call_original
      subject.messages!
    end

    context "Failure" do
      it "should not put the sender_id and messages back if processing fails bc new messages may have been processed while that process failed" do; end
    end

    context "Concurrent Access" do
      it "should work in sidekiq"
      it "should pool its connections"
      it "should reconnect if it looses its connection"
      it "should be fork() proof"
    end
  end
end