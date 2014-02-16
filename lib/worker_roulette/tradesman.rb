module WorkerRoulette
  class Tradesman
    attr_reader :sender
    def initialize(client_pool, pubsub_pool, namespace = nil)
      @client_pool = client_pool
      @pubsub_pool = pubsub_pool
      @namespace   = namespace
      @channel     = namespace || WorkerRoulette::JOB_NOTIFICATIONS
    end

    def job_board_key
      @job_board_key ||= "#{@namespace + ':' if @namespace}#{WorkerRoulette::JOB_BOARD}"
    end

    def sender_key
      @sender_key ||= "#{@namespace + ':' if @namespace}#{@sender}"
    end

    def wait_for_work_orders(on_subscribe_callback = nil, &block)
      @pubsub_pool.with do |redis|
        redis.subscribe(@channel) do |on|
          on.subscribe {on_subscribe_callback.call if on_subscribe_callback}
          on.message   {block.call(work_orders!) if block}
        end
      end
    end

    def work_orders!
      @client_pool.with do |redis|
        get_sender_for_next_job(redis)
        results = redis.multi do
          redis.lrange(sender_key, 0, -1)
          redis.del(sender_key)
          redis.zrem(job_board_key, sender_key)
        end
        ((results || []).first || []).map {|work_order| Oj.load(work_order)}
      end
    end

    def unsubscribe
      @pubsub_pool.with {|redis| redis.unsubscribe(@channel)}
    end

  private
    def get_sender_for_next_job(redis)
      @sender = (redis.zrange(job_board_key, 0, 0) || []).first.to_s
    end
  end
end