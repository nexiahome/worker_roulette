require_relative './foreman'
module WorkerRoulette
  class AForeman < Foreman
    def enqueue_work_order_without_headers(work_order, &callback)
      Lua.call(self.class.lua_enqueue_work_orders, [COUNTER_KEY, job_board_key, sender_key, @channel],
                   [@sender, Oj.dump(work_order),  WorkerRoulette::JOB_NOTIFICATIONS], &callback)
    end

  private
    def self.lua_enqueue_work_orders
      <<-HERE
        local counter_key       = KEYS[1]
        local job_board_key     = KEYS[2]
        local sender_key        = KEYS[3]
        local channel           = KEYS[4]

        local sender            = ARGV[1]
        local work_order        = ARGV[2]
        local job_notification  = ARGV[3]

        local function enqueue_work_orders(sender, work_order, job_notification)
          local result    = sender .. ' updated'
          local sender_on_job_board = redis.call('ZSCORE', job_board_key, sender_key)

          if (sender_on_job_board == false) then
            local count     = redis.call('INCR', counter_key)
            local job_added = redis.call('ZADD',job_board_key, count, sender_key)
            result    = sender .. ' added'
          end

          local work_added          = redis.call('RPUSH',sender_key, work_order)
          local job_board_update    = redis.call('PUBLISH', channel, job_notification)
          return result
        end

        return enqueue_work_orders(sender, work_order, job_notification)
      HERE
    end
  end
end
