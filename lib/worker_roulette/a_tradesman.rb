require_relative './tradesman'
module WorkerRoulette
  class ATradesman < Tradesman
    def wait_for_work_orders(on_subscribe_callback = nil, &on_message_callback)
      @redis_pubsub ||= WorkerRoulette.new_redis_pubsub #cannot use connection pool bc redis expects each obj to own its own pubsub connection for the life of the subscription
      @redis_pubsub.on(:subscribe) {|channel, subscription_count| on_subscribe_callback.call(channel, subscription_count) if on_subscribe_callback}
      @redis_pubsub.on(:message)   {|channel, message| work_orders! {|work_orders| on_message_callback.call(work_orders, message, channel)} if on_message_callback}
      @redis_pubsub.subscribe(@channel)
    end

    def work_orders!(&callback)
      @client_pool.with do |redis|
        get_sender_for_next_job(redis) do |sender_results|
          @sender = (sender_results || []).first.to_s
          redis.multi
          redis.lrange(sender_key, 0, -1)
          redis.del(sender_key)
          redis.zrem(job_board_key, sender_key)
          redis.exec do |work_orders|
            callback.call ((work_orders || []).first || []).map {|work_order| Oj.load(work_order)} if callback
          end
        end
      end
    end

    def get_lock(redis, sender, timeout, on_failure = nil, &on_success)
      @lock = EM::Hiredis::Lock.new(redis, sender, timeout)
      @lock.callback &on_success
      @lock.errback &(on_failure || proc {})
      @lock
    end

    def unsubscribe(&callback)
      deferable = @redis_pubsub.unsubscribe(@channel)
      deferable.callback do
        @redis_pubsub.close_connection
        @redis_pubsub = nil
        callback.call
      end
      deferable.errback do
        @redis_pubsub.close_connection
        @redis_pubsub = nil
        callback.call
      end
    end

    private
    def get_sender_for_next_job(redis, &callback)
      redis.zrange(job_board_key, 0, 0).callback &callback
    end
  end
end
