module WorkerRoulette
  require 'worker_roulette'
  require 'evented-spec'
  require 'rspec'
  require 'pry'

  require File.expand_path(File.join("..", "..", "lib", "worker_roulette.rb"), __FILE__)

  Dir[File.join(File.dirname(__FILE__), 'helpers', '**/*.rb')].sort.each { |file| require file.gsub(".rb", "")}

  EM::Hiredis.reconnect_timeout = 0.01

  RSpec.configure do |c|
    c.after(:each) do
      Redis.new(WorkerRoulette.start.redis_config.merge(host: '127.0.0.1')).flushdb
    end
  end
end
