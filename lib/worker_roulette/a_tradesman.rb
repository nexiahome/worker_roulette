require_relative './tradesman'
module WorkerRoulette
  class ATradesman < Tradesman
    def wait_for_work_orders(on_subscribe_callback = nil, &on_message_callback)
      @redis_pubsub ||= WorkerRoulette.new_redis_pubsub #cannot use connection pool bc redis expects each obj to own its own pubsub connection for the life of the subscription
      @redis_pubsub.on(:subscribe) {|channel, subscription_count| on_subscribe_callback.call(channel, subscription_count) if on_subscribe_callback}
      @redis_pubsub.on(:message)   {|channel, message| set_timer(on_message_callback); work_orders! {|work_orders| on_message_callback.call(work_orders, message, channel)} if on_message_callback}
      @redis_pubsub.subscribe(@channel)
    end

    def work_orders!(&callback)
      Lua.call(self.class.lua_drain_work_orders, [job_board_key, nil], [@namespace]) do |results|
        @sender = (results.first || '').split(':').first
        callback.call (results[1] || []).map {|work_order| Oj.load(work_order)} if callback
      end
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
    attr_reader :timer
    def set_timer(on_message_callback)
      return unless on_message_callback
      @timer && @timer.cancel
      @timer = EM::PeriodicTimer.new(rand(20..25)) do
        work_orders! {|work_orders| on_message_callback.call(work_orders, nil, nil)}
      end
    end

    def self.lua_drain_work_orders
      <<-HERE
        local job_board_key     = KEYS[1]
        local empty             = KEYS[2]
        local namespace         = ARGV[1]

        local function drain_work_orders(job_board_key, namespace)
          local sender_key = redis.call('ZRANGE', job_board_key, 0, 0)[1]

          if sender_key == false then
            return {}
          end

          local results = {}
          results[1] = sender_key
          results[2] = redis.call('LRANGE', sender_key, 0, -1)
          results[3] = redis.call('DEL', sender_key)
          results[4] = redis.call('ZREM', job_board_key, sender_key)
          return results
        end

        return drain_work_orders(job_board_key, namespace)
      HERE
    end
  end
end
