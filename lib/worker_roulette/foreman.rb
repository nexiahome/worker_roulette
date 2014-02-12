module WorkerRoulette
  class Foreman
    attr_reader :sender
    COUNTER_KEY = 'counter_key'

    def initialize(sender, redis_pool)
      @sender = sender
      @redis_pool = redis_pool
    end

    def job_board_key
      WorkerRoulette::JOB_BOARD
    end

    def counter_key
      COUNTER_KEY
    end

    def enqueue_work_order_without_headers(work_order)
      #Caveat Emptor: There is a race condition here, but it not serious;
      #the count may be incremented again by another process before the sender
      #is added to the job_queue. This is not a big deal bc it just means that
      #the sender's queue will be processed one slot behind it's rightful place.
      #This does not effect work_order ordering.
      @redis_pool.with do |redis|
        @count = redis.incr(COUNTER_KEY)
        redis.multi do
          redis.zadd(WorkerRoulette::JOB_BOARD, @count, sender)
          redis.rpush(sender, Oj.dump(work_order))
          redis.publish(WorkerRoulette::JOB_NOTIFICATIONS, WorkerRoulette::JOB_NOTIFICATIONS)
        end
      end
    end

    def enqueue_work_order(work_order, headers = {})
      work_order = {headers: default_headers.merge(headers), payload: work_order}
      enqueue_work_order_without_headers(work_order)
    end

    def default_headers
      Hash[sender: sender]
    end
  end
end