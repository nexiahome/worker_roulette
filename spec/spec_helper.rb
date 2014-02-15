require 'worker_roulette'
require 'em-synchrony'
require 'simplecov'
require 'simplecov-rcov'
require 'rspec'
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

module RSpec
  module Core
    class ExampleGroup

      class << self
        alias_method :run_alias, :run

        def run(reporter)
          if EM.reactor_running?
            run_alias reporter
          else
            out = nil
            EM.synchrony do
              out = run_alias reporter
              EM.stop
            end
            out
          end
        end
      end

    end
  end
end

RSpec.configure do |c|
  c.after(:each) do
    Redis.new(WorkerRoulette.redis_config).flushdb
  end
end