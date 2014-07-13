require 'worker_roulette'
# require 'ruby-prof'

def publish(iterations)
  WorkerRoulette.start(evented: false)
  work_order = {'ding dong' => "hello_foreman_" * 100}
  iterations.times do |iteration|
    sender = 'sender_' + (iteration / 2).to_i.to_s
    foreman = WorkerRoulette.foreman(sender, 'good_channel')
    foreman.enqueue_work_order(work_order)
    puts "published: #{iteration}" if iteration % 10_000 == 0
  end
end

def subscribe(iterations)
  WorkerRoulette.start(evented: false)
  @tradesman = WorkerRoulette.tradesman('good_channel')
  @received = 0
  @tradesman.wait_for_work_orders do |work|
    @start ||= Time.now
    @received += 1
    puts @received if @received % (iterations / 10) == 0
    puts "#{ iterations / (Time.now - start).to_i} reads per seconds" if @received == iterations
  end
end

def asub(iterations)
  WorkerRoulette.start(evented: true)
  @tradesman = WorkerRoulette.a_tradesman('good_channel')
  @received = 0
  @tradesman.wait_for_work_orders do |work|
    @received += work.length
    puts @received if @received % (iterations / 10) == 0
  end
end

def start(action, iterations = 1_000_000)
  EM.kqueue = true
  socket_max = 50_000
  EventMachine.set_descriptor_table_size(socket_max)

  EM.run do
    Signal.trap("INT") {
      EM.stop_event_loop
    }
    Signal.trap("TERM") {
      EM.stop_event_loop
    }

    self.send(action, iterations)
  end
end
