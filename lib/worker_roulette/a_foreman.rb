require_relative './foreman'
module WorkerRoulette
  class AForeman < Foreman
    def enqueue_work_order_without_headers(work_order, &callback)
      #Caveat Emptor: There is a race condition here, but it not serious;
      #the count may be incremented again by another process before the sender
      #is added to the job_queue. This is not a big deal bc it just means that
      #the sender's queue will be processed one slot behind it's rightful place.
      #This does not effect work_order ordering.
      @redis_pool.with do |redis|
        df = redis.eval(self.class.lua_enqueue_work_orders, 4,
                   *[COUNTER_KEY, job_board_key, sender_key, @channel],
                   *[@sender, Oj.dump(work_order),  WorkerRoulette::JOB_NOTIFICATIONS])
        df.callback &callback
        df.errback &callback
      end
    end

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
          local sender_on_job_board = redis.call('ZSCORE', job_board_key, sender)

          if (sender_on_job_board == false) then
            local count     = redis.call('INCR', counter_key)
            local job_added = redis.call('ZADD',job_board_key, count, sender)
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
