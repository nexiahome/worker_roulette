module Switchboard
  class Operator
    attr_reader :sender
    COUNTER_KEY = 'counter_key'

    def initialize(sender, redis_pool)
      @sender = sender
      @redis_pool = redis_pool
    end

    def job_board_key
      Switchboard::JOB_BOARD
    end

    def counter_key
      COUNTER_KEY
    end

    def enqueue_without_headers(message)
      #Caveat Emptor: There is a race condition here, but it not serious
      #the count may be incremented again by anothe process before the sender
      #is added to the job_queue. This is not a big deal bc it just means that
      #the sender's queue will be processed one slot behind it's rightful place.
      #This does not effect message ordering.
      @redis_pool.with do |redis|
        @count = redis.incr(COUNTER_KEY)
        redis.multi do
          redis.zadd(Switchboard::JOB_BOARD, @count, sender)
          redis.rpush(sender, Oj.dump(message))
          redis.publish(Switchboard::JOB_NOTIFICATIONS, Switchboard::JOB_NOTIFICATIONS)
        end
      end
    end

    def enqueue(message, headers = {})
      message = {headers: default_headers.merge(headers), payload: message}
      enqueue_without_headers(message)
    end

    def default_headers
      Hash[sender: sender]
    end
  end
end