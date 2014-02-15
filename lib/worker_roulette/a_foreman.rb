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
        redis.incr(COUNTER_KEY) do |count|
          @count = count ||= 1
          redis.multi
          redis.zadd(job_board_key, @count, @sender)
          redis.rpush(sender_key, Oj.dump(work_order))
          redis.publish(@channel, WorkerRoulette::JOB_NOTIFICATIONS)
          redis.exec.callback &callback
        end
      end
    end
  end
end
