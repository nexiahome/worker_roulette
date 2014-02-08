module Switchboard
  class Operator
    attr_reader :namespace, :sender, :counter_key
    def initialize(namespace, sender, redis_client)
      @namespace = namespace.to_sym
      @sender = sender
      @redis = Redis::Namespace.new(namespace, redis: redis_client)
      @counter_key = "#{sender}_counter_key"
    end

    def job_board_key
      Switchboard::JOB_BOARD
    end

    def enqueue(message)
      #Caveat Emptor: There is a race condition here, but it not serious
      #the count may be incremented again by anothe process before the sender
      #is added to the job_queue. This is not a big deal bc it just means that
      #the sender's queue will be processed one slot behind it's rightful place.
      #This does not effect message ordering.
      @count = @redis.incr(@counter_key)
      @redis.multi do
        @redis.zadd(Switchboard::JOB_BOARD, @count, sender)
        @redis.rpush(sender, message)
      end
    end
  end
end