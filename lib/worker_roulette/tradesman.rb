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

    def wait_for_work_orders(on_subscribe_callback = nil, &block)
      @pubsub_pool.with do |redis|
        redis.subscribe(WorkerRoulette::JOB_NOTIFICATIONS) do |on|
          on.subscribe {on_subscribe_callback.call if on_subscribe_callback}
          on.message   {block.call(work_orders!) if block}
        end
      end
    end

    def work_orders!
      @client_pool.with do |redis|
        get_sender_for_next_job(redis)
        results = redis.multi do
          redis.lrange(sender, 0, -1)
          redis.del(sender)
          redis.zrem(WorkerRoulette::JOB_BOARD, sender)
        end
        ((results || []).first || []).map {|work_order| Oj.load(work_order)}
      end
    end

    def unsubscribe
      @pubsub_pool.with {|redis| redis.unsubscribe}
    end

  private
    def get_sender_for_next_job(redis)
      @sender = (redis.zrange(WorkerRoulette::JOB_BOARD, 0, 0) || []).first.to_s
    end
  end
end