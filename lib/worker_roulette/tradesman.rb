module WorkerRoulette
  class Tradesman
    attr_reader :sender
    def initialize(client_pool, pubsub_pool, namespace = nil)
      @client_pool = client_pool
      @pubsub_pool = pubsub_pool
      @namespace   = namespace
      @channel     = namespace || WorkerRoulette::JOB_NOTIFICATIONS
    end

    def wait_for_work_orders(on_subscribe_callback = nil, &block)
      @pubsub_pool.with do |redis|
        redis.subscribe(@channel) do |on|
          on.subscribe {on_subscribe_callback.call if on_subscribe_callback}
          on.message   {self.unsubscribe; block.call(work_orders!) if block}
        end
      end
    end

    def work_orders!(&callback)
      Lua.call(self.class.lua_drain_work_orders, [job_board_key, nil], [@namespace]) do |results|
        @sender = (results.first || '').split(':').first
        work = (results[1] || []).map {|work_order| WorkerRoulette.load(work_order)}
        callback.call work if callback
        work
      end
    end

    def unsubscribe
      @pubsub_pool.with {|redis| redis.unsubscribe(@channel)}
    end

    def job_board_key
      @job_board_key ||= WorkerRoulette.job_board_key(@namespace)
    end

  private
    def sender_key
      @sender_key = WorkerRoulette.sender_key(@sender, @namespace)
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

    def get_sender_for_next_job(redis)
      @sender = (redis.zrange(job_board_key, 0, 0) || []).first.to_s
    end
  end
end