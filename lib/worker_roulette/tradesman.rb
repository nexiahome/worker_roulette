module WorkerRoulette
  class Tradesman
    attr_reader :last_sender
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
          on.message   {block.call(work_orders! + work_orders!) if block}
        end
      end
    end

    def work_orders!(&callback)
      Lua.call(self.class.lua_drain_work_orders, [job_board_key, @last_sender], [nil]) do |results|
        results ||= []
        @last_sender = (results.first || '').split(':').first
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
    def self.lua_drain_work_orders
      <<-HERE
        local job_board_key     = KEYS[1]
        local last_sender_key   = KEYS[2]
        local sender_key        = ARGV[1]

        local function drain_work_orders(job_board_key, last_sender_key, sender_key)
          if last_sender_key ~= "" and last_sender_key ~= nil then
            local last_sender_lock_key = 'L*:' .. last_sender_key
            redis.call('DEL', last_sender_lock_key)
          end

          if (not sender_key) or (sender_key == "") then
            sender_key = redis.call('ZRANGE', job_board_key, 0, 0)[1]
            if (not sender_key) or (sender_key == "") then
              return {}
            end
          end

          local lock_key = 'L*:' .. sender_key
          local locked   = (redis.call('GET', lock_key) == 'L')

          if not locked then
            local results = {}
            results[1] = sender_key
            results[2] = redis.call('LRANGE', sender_key, 0, -1)
            redis.call('DEL', sender_key)
            redis.call('ZREM', job_board_key, sender_key)
            redis.call('SET', lock_key, 'L', 'EX', 1, 'NX')
            return results
          else
            local sender_index    = redis.call('ZRANK', job_board_key, sender_key)
            local next_index      = sender_index + 1
            local next_sender_key = redis.call('ZRANGE', job_board_key, next_index, next_index)[1]
            if next_sender_key then
              return drain_work_orders(job_board_key, "", next_sender_key)
            else
              return {}
            end
          end
        end

        return drain_work_orders(job_board_key, last_sender_key, "")
      HERE
    end
  end
end