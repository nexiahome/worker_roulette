require_relative "preprocessor"

module WorkerRoulette
  class Tradesman
    include Preprocessor
    attr_reader :last_sender, :remaining_jobs, :timer, :preprocessors, :channel

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

    LUA_DRAIN_WORK_ORDERS_FOR_SENDER = <<-THERE
      local empty_string     = ""
      local job_board_key    = KEYS[1]
      local sender_key       = KEYS[2] or empty_string
      local redis_call       = redis.call
      local lock_key_prefix  = "L*:"
      local lock_value       = 1
      local get              = "GET"
      local del              = "DEL"
      local lrange           = "LRANGE"
      local zrange           = "ZRANGE"
      local zrem             = "ZREM"

      local function drain_work_orders_for_sender(job_board_key, sender_key)

        if sender_key == empty_string then
          return {empty_string, {}, 0}
        end

        local lock_key = lock_key_prefix .. sender_key
        was_locked = redis_call(get, lock_key)

        if was_locked == 1 then
          local work_orders = redis_call(lrange, sender_key, 0, -1)
          redis_call(del, sender_key)

          redis_call(zrem, job_board_key, sender_key)

          return { sender_key, work_orders }
        else
          return { sender_key, {} }
        end
      end

      return drain_work_orders_for_sender(job_board_key, sender_key)
    THERE

    def initialize(redis_pool, evented, namespace = nil, polling_time = WorkerRoulette::DEFAULT_POLLING_TIME, preprocessors = [])
      @evented        = evented
      @polling_time   = polling_time
      @preprocessors  = preprocessors
      @redis_pool     = redis_pool
      @namespace      = namespace
      @channel        = namespace || WorkerRoulette::JOB_NOTIFICATIONS
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
        @last_sender    = sender_key

        QueueMetricTracker.track_all(results) if work_orders.any?
        work = work_orders.map { |wo| preprocess(WorkerRoulette.load(wo), channel) }
        callback.call work if callback
        work
      end
    end

    def get_more_work_for_last_sender(&on_message_callback)
      return unless on_message_callback
      more_work_orders! do |work|
        on_message_callback.call(work) if work.any?
        if @evented
          evented_drain_work_queue!(&on_message_callback)
        else
          non_evented_drain_work_queue!(&on_message_callback)
        end
      end
    end

    def more_work_orders!(&callback)
      @lua.call(LUA_DRAIN_WORK_ORDERS_FOR_SENDER, [job_board_key, @last_sender]) do |results|
        sender_key      = results[0]
        raise "wrong sender key returned from LUA_DRAIN_WORK_ORDERS_FOR_SENDER" unless sender_key == @last_sender
        work_orders     = results[1]
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
        sleep 2
        wait_for_work_orders(&on_message_callback)
      end
    end
  end
end
