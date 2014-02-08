require 'switchboard'
# require 'simplecov'
# require 'simplecov-rcov'
# class SimpleCov::Formatter::MergedFormatter
#   def format(result)
#      SimpleCov::Formatter::HTMLFormatter.new.format(result)
#      SimpleCov::Formatter::RcovFormatter.new.format(result)
#   end
# end
# SimpleCov.formatter = SimpleCov::Formatter::MergedFormatter
# SimpleCov.start

require File.expand_path(File.join("..", "..", "lib", "switchboard.rb"), __FILE__)
include Switchboard

Dir[File.join(File.dirname(__FILE__), 'helpers', '**/*.rb')].sort.each { |file| require file.gsub(".rb", "")}

# RSpec.configure do |c|
#   after(:each) do
#     Redis.new.flushall
#   end
# end