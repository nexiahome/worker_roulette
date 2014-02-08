# encoding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'switchboard/version'

Gem::Specification.new do |spec|
  spec.name          = "switchboard"
  spec.version       = Switchboard::VERSION
  spec.authors       = ["Paul Saieg"]
  spec.email         = ["paul.saieg@irco.com"]
  spec.description   = %q{Write a gem description}
  spec.summary       = %q{Write a gem summary}
  spec.homepage      = ""

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'redis'
  spec.add_dependency 'redis-namespace'

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"
  spec.add_development_dependency 'rspec'
  spec.add_development_dependency 'pry-debugger'
  # spec.add_development_dependency 'guard'
  # spec.add_development_dependency 'guard-rspec'
  # spec.add_development_dependency 'simplecov'
  # spec.add_development_dependency 'simplecov-rcov'
  # spec.add_development_dependency 'rspec_junit_formatter'
  # spec.add_development_dependency 'sidekiq'
end
