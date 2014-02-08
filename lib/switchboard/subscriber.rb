module Switchboard
  class Subscriber
    attr_reader :namespace, :sender
    def initialize(namespace, redis_client)
      @namespace = namespace.to_sym
      @redis = Redis::Namespace.new(namespace, redis: redis_client)
    end

    def job_board_key
      Switchboard::JOB_BOARD
    end

    def messages!
      setup
      results = @redis.multi do
        @redis.lrange(sender, 0, -1)
        @redis.del(sender)
        @redis.zrem(Switchboard::JOB_BOARD, sender)
      end
      (results || []).first
    end

  private
    def setup
      @sender ||= (@redis.zrevrange(Switchboard::JOB_BOARD, -1, -1) || ['']).first.to_sym
    end
  end
end