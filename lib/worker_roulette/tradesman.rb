require 'timers'

module WorkerRoulette
  class Tradesman
    attr_reader :last_sender, :remaining_jobs, :timer

    LUA_DRAIN_WORK_ORDERS = <<-HERE
      local empty_string      = ""
      local job_board_key     = KEYS[1]
      local last_sender_key   = KEYS[2] or empty_string
      local sender_key        = ARGV[1] or empty_string
      local redis_call        = redis.call
      local lock_key_prefix   = "L*:"
      local lock_value        = 1
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

        --kill lock for last_sender_key
        if last_sender_key ~= empty_string then
          local last_sender_lock_key = lock_key_prefix .. last_sender_key
          redis_call(del, last_sender_lock_key)
        end

        if sender_key == empty_string then
          sender_key = redis_call(zrange, job_board_key, 0, 0)[1] or empty_string

          -- return if job_board is empty
          if sender_key == empty_string then
            return {empty_string, {}, 0}
          end
        end

        local lock_key       = lock_key_prefix .. sender_key
        local was_not_locked = redis_call(set, lock_key, lock_value, ex, 3, nx)

        if was_not_locked then
          local work_orders    = redis_call(lrange, sender_key, 0, -1)
          redis_call(del, sender_key)

          redis_call(zrem, job_board_key, sender_key)
          local remaining_jobs = redis_call(zcard, job_board_key) or 0

          return {sender_key, work_orders, remaining_jobs}
        else
          local sender_index    = redis_call(zrank, job_board_key, sender_key)
          local next_index      = sender_index + 1
          local next_sender_key = redis_call(zrange, job_board_key, next_index, next_index)[1]
          if next_sender_key then
            return drain_work_orders(job_board_key, empty_string, next_sender_key)
          else
            -- return if job_board is empty
            return {empty_string, {}, 0}
          end
        end
      end

      return drain_work_orders(job_board_key, last_sender_key, empty_string)
    HERE

    def initialize(redis_pool, evented, namespace = nil, polling_time = WorkerRoulette::DEFAULT_POLLING_TIME)
      @evented        = evented
      @polling_time   = polling_time
      @redis_pool     = redis_pool
      @namespace      = namespace
      @channel        = namespace || WorkerRoulette::JOB_NOTIFICATIONS
      @timer          = Timers::Group.new
      @lua            = Lua.new(@redis_pool)
      @remaining_jobs = 0
    end

    def wait_for_work_orders(&on_message_callback)
      return unless on_message_callback
      work_orders! do |work|
        on_message_callback.call(work) if work.any?
        if @evented
          evented_drain_work_queue!(&on_message_callback)
        else
          non_evented_drain_work_queue!(&on_message_callback)
        end
      end
    end

    def work_orders!(&callback)
      @lua.call(LUA_DRAIN_WORK_ORDERS, [job_board_key, @last_sender], [nil]) do |results|
        sender_key      = results[0]
        work_orders     = results[1]
        @remaining_jobs = results[2]
        @last_sender    = sender_key.split(':').last
        work            = work_orders.map {|work_order| WorkerRoulette.load(work_order)}
        callback.call work if callback
        work
      end
    end

    def job_board_key
      @job_board_key ||= WorkerRoulette.job_board_key(@namespace)
    end

    private

    def evented_drain_work_queue!(&on_message_callback)
      if remaining_jobs > 0
        EM.next_tick {wait_for_work_orders(&on_message_callback)}
      else
        EM.add_timer(@polling_time) { wait_for_work_orders(&on_message_callback) }
      end
    end

    def non_evented_drain_work_queue!(&on_message_callback)
      if remaining_jobs > 0
        wait_for_work_orders(&on_message_callback)
      else
        @timer.after(@polling_time) { wait_for_work_orders(&on_message_callback) }
        @timer.wait
      end
    end
  end
end
