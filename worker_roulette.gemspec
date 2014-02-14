# encoding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'worker_roulette/version'

Gem::Specification.new do |spec|
  spec.name          = "worker_roulette"
  spec.version       = WorkerRoulette::VERSION
  spec.authors       = ["Paul Saieg"]
  spec.email         = ["classicist@gmail.com"]
  spec.description   = %q{Write a gem description}
  spec.summary       = %q{Write a gem summary}
  spec.homepage      = "https://github.com/nexiahome/worker_roulette"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'oj'
  spec.add_dependency 'redis', '~> 3.0.7'
  spec.add_dependency 'hiredis', '~> 0.4.5'
  spec.add_dependency 'em-synchrony'
  spec.add_dependency 'connection_pool'

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency 'rspec'
  spec.add_development_dependency 'pry-debugger'
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency 'simplecov-rcov'
  spec.add_development_dependency 'rspec_junit_formatter'
end
