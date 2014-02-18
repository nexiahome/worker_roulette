require 'worker_roulette'
require 'evented-spec'
require 'simplecov'
require 'simplecov-rcov'
require 'rspec'
require 'pry'

class SimpleCov::Formatter::MergedFormatter
  def format(result)
     SimpleCov::Formatter::HTMLFormatter.new.format(result)
     SimpleCov::Formatter::RcovFormatter.new.format(result)
  end
end
SimpleCov.formatter = SimpleCov::Formatter::MergedFormatter
SimpleCov.start

require File.expand_path(File.join("..", "..", "lib", "worker_roulette.rb"), __FILE__)
include WorkerRoulette

Dir[File.join(File.dirname(__FILE__), 'helpers', '**/*.rb')].sort.each { |file| require file.gsub(".rb", "")}

EM::Hiredis.reconnect_timeout = 0.01

RSpec.configure do |c|
  c.after(:each) do
    Redis.new(WorkerRoulette.redis_config).flushdb
  end
end