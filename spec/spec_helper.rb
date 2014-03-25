require 'worker_roulette'
require 'evented-spec'
require 'rspec'
require 'pry'

require File.expand_path(File.join("..", "..", "lib", "worker_roulette.rb"), __FILE__)
include WorkerRoulette

Dir[File.join(File.dirname(__FILE__), 'helpers', '**/*.rb')].sort.each { |file| require file.gsub(".rb", "")}

EM::Hiredis.reconnect_timeout = 0.01

RSpec.configure do |c|
  c.after(:each) do
    Redis.new(WorkerRoulette.redis_config).flushdb
  end
end