module WorkerRoulette
  class Foreman
    attr_reader :sender, :namespace, :channel

    LUA_ENQUEUE_WORK_ORDERS = <<-HERE
        local counter_key       = KEYS[1]
        local job_board_key     = KEYS[2]
        local sender_key        = KEYS[3]
        local channel           = KEYS[4]

        local work_order        = ARGV[1]
        local job_notification  = ARGV[2]
        local redis_call        = redis.call
        local zscore            = 'ZSCORE'
        local incr              = 'INCR'
        local zadd              = 'ZADD'
        local rpush             = 'RPUSH'
        local publish           = 'PUBLISH'
        local zcard             = 'ZCARD'
        local del               = 'DEL'

        local function enqueue_work_orders(work_order, job_notification)
          redis_call(rpush, sender_key, work_order)

          -- called when a work from a new sender is added
          if (redis_call(zscore, job_board_key, sender_key) == false) then
            local count     = redis_call(incr, counter_key)
            redis_call(zadd, job_board_key, count, sender_key)
          end
        end

        enqueue_work_orders(work_order, job_notification)
    HERE

    def initialize(redis_pool, sender, namespace = nil)
      @sender     = sender
      @namespace  = namespace
      @redis_pool = redis_pool
      @channel    = namespace || WorkerRoulette::JOB_NOTIFICATIONS
      @lua        = Lua.new(@redis_pool)
    end

    def enqueue_work_order(work_order, headers = {}, &callback)
      work_order = {'headers' => default_headers.merge(headers), 'payload' => work_order}
      enqueue_work_order_without_headers(work_order, &callback)
    end

    def enqueue_work_order_without_headers(work_order, &callback)
      @lua.call(LUA_ENQUEUE_WORK_ORDERS, [counter_key, job_board_key, sender_key, @channel],
               [WorkerRoulette.dump(work_order),  WorkerRoulette::JOB_NOTIFICATIONS], &callback)
    end

    def job_board_key
      @job_board_key ||= WorkerRoulette.job_board_key(@namespace)
    end

    def counter_key
      @counter_key ||= WorkerRoulette.counter_key(@namespace)
    end

    def sender_key
      @sender_key ||= WorkerRoulette.sender_key(@sender, @namespace)
    end

    private

    def default_headers
      Hash['sender' => sender]
    end
  end
end