require "spec_helper"

describe Switchboard do
  let(:sender) {:katie_80}
  let(:namespace) {:sixties_telephony}
  let(:messages) {["hello", "operator"]}
  let(:default_headers) {Hash[headers: {sender: sender}]}
  let(:hello_message) {Hash[payload: "hello"]}
  let(:operator_message) {Hash[payload: "operator"]}
  let(:messages_with_headers) {default_headers.merge({payload: messages})}
  let(:jsonized_messages_with_headers) {[Oj.dump(messages_with_headers)]}
  let(:raw_redis_client) {Redis.new}
  let(:raw_redis_subscriber) {Redis.new}
  let(:redis) {Redis::Namespace.new(namespace, redis: raw_redis_client)}
  let(:redis_subscriber) {Redis::Namespace.new(namespace, redis: raw_redis_subscriber)}

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
      redis.lrange(sender, 0, -1).should == messages.map {|m| Oj.dump(default_headers.merge({payload: m})) }
    end

    it "should enqueue an array of messages without headers in the sender's slot in the switchboard" do
      subject.enqueue_without_headers(messages)
      redis.lrange(sender, 0, -1).should == [Oj.dump(messages)]
    end

    it "should enqueue an array of messages with default headers in the sender's slot in the switchboard" do
      subject.enqueue(messages)
      redis.lrange(sender, 0, -1).should == jsonized_messages_with_headers
    end

    it "should enqueue an array of messages with additional headers in the sender's slot in the switchboard" do
      extra_headers = {foo: :bars}
      subject.enqueue(messages, extra_headers)
      messages_with_headers[:headers].merge!(extra_headers)
      redis.lrange(sender, 0, -1).should == [Oj.dump(messages_with_headers)]
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
      subject.enqueue(messages.last)
      redis.get(subject.counter_key).should == "2"
    end

    it "should publish a notification that a new job is ready" do
      result = nil
      redis_subscriber.subscribe(Switchboard::JOB_NOTIFICATIONS) do |on|
        on.subscribe do |channel, subscription|
          subject.enqueue(messages)
        end

        on.message do |channel, notification|
          result = notification
          redis_subscriber.unsubscribe(Switchboard::JOB_NOTIFICATIONS)
        end
     end

      result.should == Switchboard::JOB_NOTIFICATIONS
    end
  end

  context Subscriber do
    let(:operator) {Operator.new(namespace, sender, raw_redis_client)}
    let(:subject)  {Subscriber.new(namespace, raw_redis_client, raw_redis_subscriber)}

    before do
      operator.enqueue(messages)
    end

    it "should be working on behalf of a sender" do
      subject.messages!
      subject.sender.should == sender
    end

    it "should be scoped to a namespace" do
      subject.namespace.should == namespace
    end

    it "should be injected with a raw_redis_client so it can do is work" do
      raw_redis_client.should_receive(:lrange).and_call_original
      subject.messages!
    end

    it "should drain all the messages from the sender's slot in the switchboard" do
      subject.messages!.should == [messages_with_headers]
      subject.messages!.should == []
      subject.messages!.should == [] #does not throw an error if queue is alreay empty
    end

    it "should drain all the messages from the sender's slot in the switchboard" do
      operator.enqueue(messages)
      subject.messages!.should == [messages_with_headers, messages_with_headers]
      subject.messages!.should == []
      subject.messages!.should == [] #does not throw an error if queue is alreay empty
    end

    it "should take the most recent sender_id off the job board" do
      redis.zrange(subject.job_board_key, 0, -1).should == [sender.to_s]
      subject.messages!
      redis.zrange(subject.job_board_key, 0, -1).should == []
    end

    it "should get the sender and message list transactionally" do
      raw_redis_client.should_receive(:multi).and_call_original
      subject.messages!
    end

    it "should get the messages from the next sender's slot when a new job is ready" do
      subject.messages!
      subject.should_receive(:messages!).and_call_original
      publisher = -> {operator.enqueue(messages)}
      subject.wait_for_messages(publisher) do |redis_messages|
        redis_messages.should == [messages_with_headers]
      end
    end

    context "Failure" do
      it "should not put the sender_id and messages back if processing fails bc new messages may have been processed while that process failed" do; end
    end

    context "Concurrent Access" do

      it "should pool its connections" do
        connections_outside_of_pool = 4
        pool_size = 10
        Switchboard.start(pool_size)
        Array.new(100) {Thread.new {Switchboard.pooled_redis_subscriber.get("foo")}}.each(&:join)
        Switchboard.pooled_redis_client.info["connected_clients"].to_i.should == (pool_size + connections_outside_of_pool)
      end

      it "should be fork() proof"
      it "should reconnect if it looses its connection"
      it "should use optionally non-blocking I/O"
      it "should checkout a readlock for a queue and put it back when its done processing; lock should expire after 5 minutes?"
      it "should work in sidekiq"
    end
  end
end