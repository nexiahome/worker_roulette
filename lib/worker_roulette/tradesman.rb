module WorkerRoulette
  class Tradesman
    attr_reader :sender
    def initialize(client_pool, pubsub_pool)
      @client_pool = client_pool
      @pubsub_pool = pubsub_pool
    end

    def job_board_key
      WorkerRoulette::JOB_BOARD
    end

    def wait_for_messages(on_subscribe_callback = nil, &block)
      @pubsub_pool.with do |redis|
        redis.subscribe(WorkerRoulette::JOB_NOTIFICATIONS) do |on|
          on.subscribe {on_subscribe_callback.call if on_subscribe_callback}
          on.message   {redis.unsubscribe; block.call(messages!) if block}
        end
      end
    end

    def messages!
      @client_pool.with do |redis|
        get_sender_for_next_job(redis)
        results = redis.multi do
          redis.lrange(sender, 0, -1)
          redis.del(sender)
          redis.zrem(WorkerRoulette::JOB_BOARD, sender)
        end
        ((results || []).first || []).map {|message| Oj.load(message)}
      end
    end

  private
    def get_sender_for_next_job(redis)
      @sender = (redis.zrange(WorkerRoulette::JOB_BOARD, 0, 0) || []).first.to_s
    end
  end
end