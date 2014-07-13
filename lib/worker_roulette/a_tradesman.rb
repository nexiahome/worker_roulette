require_relative './tradesman'
module WorkerRoulette
  class ATradesman < Tradesman
    attr_reader :timer

    def wait_for_work_orders(on_subscribe_callback = nil, &on_message_callback)
      @redis_pubsub ||= WorkerRoulette.new_redis_pubsub #cannot use connection pool bc redis expects each obj to own its own pubsub connection for the life of the subscription
      @redis_pubsub.on(:subscribe) {|channel, subscription_count| on_subscribe_callback.call(channel, subscription_count) if on_subscribe_callback}
      @redis_pubsub.on(:message)   do |channel, message|
        # puts "got #{message}"
        EM.add_timer(2) {puts "started new work"; drain_queue(&on_message_callback)}
      end

      drain_queue(&on_message_callback)

      @redis_pubsub.subscribe(@channel)
    end

    def unsubscribe(&callback)
      deferable = @redis_pubsub.unsubscribe(@channel)
      deferable.callback do
        @redis_pubsub.close_connection
        @redis_pubsub = nil
        callback.call
      end
      deferable.errback do
        @redis_pubsub.close_connection
        @redis_pubsub = nil
        callback.call
      end
    end

    def drain_queue(&on_message_callback)
      return unless on_message_callback
      work_orders! do |work|
        # puts "remaining_jobs: #{remaining_jobs}" if remaining_jobs % 5000 == 0 || remaining_jobs < 5
        on_message_callback.call([work]) if work.any?
        EM.next_tick {drain_queue(&on_message_callback)} if remaining_jobs > 0
      end
    end

    private

    def get_messages(message, channel, on_message_callback)
      return unless on_message_callback
      work_orders! do |work_orders_1|
        work_orders! do |work_orders|
          on_message_callback.call(work_orders_1 + work_orders, message, channel)
        end
      end
    end

    def set_timer(on_message_callback)
      return if (@timer || !on_message_callback)
      @timer = EM::PeriodicTimer.new(rand(20..25)) do
        work_orders! {|work_orders| on_message_callback.call(work_orders, nil, nil)}
      end
    end
  end
end
