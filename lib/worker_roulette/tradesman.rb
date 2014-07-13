module WorkerRoulette
  class Tradesman
    attr_reader :last_sender, :remaining_jobs

    LUA_DRAIN_WORK_ORDERS = <<-HERE
      local empty_string      = ""
      local job_board_key     = KEYS[1]
      local last_sender_key   = KEYS[2] or empty_string
      local sender_key        = ARGV[1] or empty_string
      local redis_call        = redis.call
      local lock_key_prefix   = "L*:"
      local lock_value        = "L"
      local ex                = "EX"
      local nx                = "NX"
      local get               = "GET"
      local set               = "SET"
      local del               = "DEL"
      local lrange            = "LRANGE"
      local zrank             = "ZRANK"
      local zrange            = "ZRANGE"
      local zrem              = "ZREM"
      local zcard             = 'ZCARD'

      local function drain_work_orders(job_board_key, last_sender_key, sender_key)
      if last_sender_key ~= empty_string then
        local last_sender_lock_key = lock_key_prefix .. last_sender_key
        redis_call(del, last_sender_lock_key)
      end

      if sender_key == empty_string then
        sender_key = redis_call(zrange, job_board_key, 0, 0)[1] or empty_string
        if sender_key == empty_string then
          return {}
        end
      end

      local lock_key = lock_key_prefix .. sender_key
      local locked   = (redis_call(get, lock_key) == lock_value)

      if not locked then
        local work_orders    = redis_call(lrange, sender_key, 0, -1)
        redis_call(del, sender_key)

        redis_call(zrem, job_board_key, sender_key)
        local remaining_jobs = redis_call(zcard, job_board_key)

        redis_call(set, lock_key, lock_value, ex, 3, nx)

        return {sender_key, work_orders, remaining_jobs}
      else
        local sender_index    = redis_call(zrank, job_board_key, sender_key)
        local next_index      = sender_index + 1
        local next_sender_key = redis_call(zrange, job_board_key, next_index, next_index)[1]
        if next_sender_key then
          return drain_work_orders(job_board_key, empty_string, next_sender_key)
        else
          return {}
        end
      end
    end

    return drain_work_orders(job_board_key, last_sender_key, empty_string)
    HERE

    def initialize(client_pool, pubsub_pool, namespace = nil)
      @client_pool = client_pool
      @pubsub_pool = pubsub_pool
      @namespace   = namespace
      @channel     = namespace || WorkerRoulette::JOB_NOTIFICATIONS
      @remaining_jobs = 0
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
      Lua.call(LUA_DRAIN_WORK_ORDERS, [job_board_key, @last_sender], [nil]) do |results|
        # puts "Gots stuff #{results}"
        sender_key      = results[0]
        work_orders     = results[1]
        @remaining_jobs = results[2]  if results[2]
        @last_sender    = sender_key  ? sender_key.split(':').first : nil
        work            = work_orders ? work_orders.map {|work_order| WorkerRoulette.load(work_order)} : []
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
  end
end