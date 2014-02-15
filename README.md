# WorkerRoulette

WorkerRoulette is designed to allow large numbers of unique devices, processes, users, or whatever communicate over individual channels without messing up the order of their messages. WorkerRoulette was created to solve two otherwise hard problems. First, other messaging solutions (I'm looking at you RabbitMQ) are not designed to  handle very large numbers of queues (30,000+); because WorkerRoulette is built on top of Redis, we have successfully tested it running with millions of queues. Second, other messaging systems assume one (or more) of three things: 1. Your message consumers know the routing key of messages they are interested in processing; 2. Your messages can wait so that only one consumer is processed at a time; 3. You love to complicated write code to put your messages back in order. Sometimes, none of these things is true and that is where WorkerRoulette comes in.

WorkerRoulette lets you have thousands of competing consumers (distrubted over as many machines as you'd like) processing ordered messages from millions of totally unknown message providers. It does all this and ensures that the messages sent from each message provider are processed in exactly the order it sent them.

## General Usage
```ruby
size_of_connection_pool = 100
redis_config = {host: 'localhost', timeout: 5, db: 1}

#Start it up
WorkerRoulette.start(size_of_connection_pool, redis_config)

#Enqueue some work
sender_id = :shady
foreman = WorkerRoulette.foreman(sender_id)
foreman.enqueue_work_order(['hello', 'foreman'])

#Pull it off
tradesman = WorkerRoulette.tradesman
messages = tradesman.work_orders! #drain the queue of the next available sender
messages.first # => ['hello', 'foreman']

#Enqueue some more from someone else
other_sender_id = :the_real_slim_shady
other_foreman = WorkerRoulette.foreman(other_sender_id)
other_foreman.enqueue_work_order({'can you get me' => 'the number nine?'})

#Have the same worker pull that off
messages = tradesman.work_orders! #drain the queue of the next available sender
messages.first # => {'can you get me' => 'the number nine?'}

#Have your workers wait for work to come in
on_subscribe_callback = -> do
  puts "Huzzah! We're listening!"
  foreman.enqueue_work_order('will I see you later?')
  foreman.enqueue_work_order('can you give me back my dime?')
end


#And they will pull it off as it comes, as long as it comes
#(This is a blocking operation, so it is best in Threads or EventMachine.next_tick)
tradesman.wait_for_work_orders(on_subscribe_callback) do |messages| #drain the queue of the next available sender
  messages # => ['will I see you later', 'can you give me back my dime?']
end
```

## Channels
You can also namespace your work orders over a channel, in case you have several sorts of competing consumers who should not step on each other's toes:
```ruby
tradesman         = WorkerRoulette.tradesman('good_channel')
tradesman.should_receive(:work_orders!).and_call_original

good_foreman      = WorkerRoulette.foreman('foreman', 'good_channel')
bad_foreman       = WorkerRoulette.foreman('foreman', 'bad_channel')

publish  = -> do
  good_foreman.enqueue_work_order('some old fashion work')
  bad_foreman.enqueue_work_order('evil biddings you should not carry out') #channels let us ignore his evil orders
end

tradesman.wait_for_work_orders(publish) do |work|
  work.to_s.should match("some old fashion work") #only got the work from the good foreman
  tradesman.unsubscribe
end

```

##Caveat Emptor
While WorkerRoulette does promise to keep the messages of each consumer processed in order by competing consumers, it does NOT guarantee the order in which the queues themselves will be processed. In general, work is processed in a FIFO order, but for performance reasons this has been left a loose FIFO. For example, if Abdul enqueue_work_orders some ordered messages ('1', '2', and '3') and then so do Mark and Wanda, Mark's messages may be processed first, then it would likely be Abdul's, and then Wanda's. However, even though Mark jumped the line, Abdul's messages will still be processed the order he enqueue_work_orderd them ('1', '2', then '3').

## Installation

Add this line to your application's Gemfile:

    gem 'worker_roulette'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install worker_roulette

## Run the specs

    $ bundle exec rake spec:ci

## Run the performance tests

    $ bundle exec rake spec:perf


## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
