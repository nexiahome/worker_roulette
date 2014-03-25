#!/usr/bin/env rake
require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new

task :default => :spec
task :test => :spec

def rspec_out_file
  require 'rspec_junit_formatter'
  "-f RspecJunitFormatter -o results.xml"
end

namespace :spec do
  desc "Run all unit and integration tests"
  task :ci do
    rspec_out_file = nil
    sh "bundle exec rspec #{rspec_out_file} spec"
  end

  desc "Run perf tests"
  task :perf do
    rspec_out_file = nil
    sh "bundle exec ruby ./spec/benchmark/perf_test.rb"
  end

  desc "Run all tests and generate coverage xml"
  task :cov do
    sh "bundle exec rspec #{rspec_out_file} spec"
  end
end
