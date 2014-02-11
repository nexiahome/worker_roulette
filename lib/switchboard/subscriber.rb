module Switchboard
  class Subscriber
    attr_reader :namespace, :sender
    def initialize(namespace, redis_client, redis_subscriber)
      @namespace = namespace.to_s
      @redis = Redis::Namespace.new(namespace, redis: redis_client)
      @redis_subscriber = Redis::Namespace.new(namespace, redis: redis_subscriber)
    end

    def job_board_key
      Switchboard::JOB_BOARD
    end

    def wait_for_messages(on_subscribe_callback = nil, &block)
      @redis_subscriber.subscribe(Switchboard::JOB_NOTIFICATIONS) do |on|
        on.subscribe {on_subscribe_callback.call if on_subscribe_callback}
        on.message { @redis_subscriber.unsubscribe; block.call(messages!) if block }
      end
    end

    def messages!
      get_sender_for_next_job
      results = @redis.multi do
        @redis.lrange(sender, 0, -1)
        @redis.del(sender)
        @redis.zrem(Switchboard::JOB_BOARD, sender)
      end
      ((results || []).first || []).map {|message| Oj.load(message)}
    end

  private
    def get_sender_for_next_job
      @sender = (@redis.zrange(Switchboard::JOB_BOARD, 0, 0) || []).first.to_s
    end
  end
end