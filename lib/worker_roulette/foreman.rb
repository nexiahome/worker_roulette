module WorkerRoulette
  class Foreman
    attr_reader :sender
    COUNTER_KEY = 'counter_key'

    def initialize(sender, redis_pool, namespace = nil)
      @sender     = sender
      @namespace  = namespace
      @redis_pool = redis_pool
      @channel    = namespace || WorkerRoulette::JOB_NOTIFICATIONS
    end

    def job_board_key
      @job_board_key ||= "#{@namespace + ':' if @namespace}#{WorkerRoulette::JOB_BOARD}"
    end

    def sender_key
      @sender_key ||= "#{@namespace + ':' if @namespace}#{@sender}"
    end

    def counter_key
      @counter_key ||= "#{@namespace + ':' if @namespace}#{COUNTER_KEY}"
    end

    def enqueue_work_order_without_headers(work_order, &callback)
      #Caveat Emptor: There is a race condition here, but it not serious;
      #the count may be incremented again by another process before the sender
      #is added to the job_queue. This is not a big deal bc it just means that
      #the sender's queue will be processed one slot behind it's rightful place.
      #This does not effect work_order ordering.
      @redis_pool.with({}) do |redis|
        @count = redis.incr(COUNTER_KEY)
        redis.multi do
          redis.zadd(job_board_key, @count, @sender)
          redis.rpush(sender_key, Oj.dump(work_order))
          redis.publish(@channel, WorkerRoulette::JOB_NOTIFICATIONS)
        end
      end
    end

    def enqueue_work_order(work_order, headers = {}, &callback)
      work_order = {'headers' => default_headers.merge(headers), 'payload' => work_order}
      enqueue_work_order_without_headers(work_order, &callback)
    end

    def default_headers
      Hash['sender' => sender]
    end
  end
end