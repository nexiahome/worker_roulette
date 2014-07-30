# encoding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'worker_roulette/version'

Gem::Specification.new do |spec|
  spec.name          = 'worker_roulette'
  spec.version       = WorkerRoulette::VERSION
  spec.authors       = ['Paul Saieg']
  spec.email         = ['classicist@gmail.com']
  spec.description   = %q{Pub Sub Queue for Redis that ensures ordered processing}
  spec.summary       = %q{Pub Sub Queue for Redis that ensures ordered processing}
  spec.homepage      = 'https://github.com/nexiahome/worker_roulette'

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(spec)/})
  spec.require_paths = ['lib']

  spec.add_dependency 'oj'
  spec.add_dependency 'redis', '~> 3.0.7'
  spec.add_dependency 'hiredis', '~> 0.4.5'
  spec.add_dependency 'em-hiredis', '~> 0.2.1'
  spec.add_dependency 'connection_pool'
  spec.add_dependency 'eventmachine', '~> 1.0.3'
  spec.add_dependency 'timers', '~> 4.0.0'

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rspec', '~> 3.0.0'
  spec.add_development_dependency 'pry-byebug'
  spec.add_development_dependency 'simplecov'
  spec.add_development_dependency 'simplecov-rcov'
  spec.add_development_dependency 'rspec_junit_formatter'
  spec.add_development_dependency 'guard'
  spec.add_development_dependency 'guard-rspec'
end
