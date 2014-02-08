#!/usr/bin/env rake
require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new

task :default => :spec
task :test => :spec

# def rspec_out_file
#   require 'rspec_junit_formatter'
#   "-f RspecJunitFormatter -o results.xml"
# end

desc "Run all unit and integration tests"
task :'spec:ci' do
  rspec_out_file = nil
  sh "bundle exec rspec #{rspec_out_file} spec"
end
