# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'humboldt/version'


Gem::Specification.new do |s|
  s.name        = 'humboldt'
  s.version     = Humboldt::VERSION.dup
  s.platform    = 'java'
  s.authors     = ['Theo Hultberg']
  s.email       = ['theo@burtcorp.com']
  s.homepage    = 'http://github.com/burtcorp/humboldt'
  s.summary     = %q{}
  s.description = %q{}

  s.rubyforge_project = 'humboldt'

  s.add_dependency 'rubydoop'
  s.add_dependency 'json'
  s.add_dependency 'thor'
  s.add_dependency 'aws-sdk', '~> 1.16.0'
  s.add_dependency 'jruby-openssl'
  
  s.files         = Dir['lib/**/*.rb'] + Dir['bin/*'] + Dir['config/**/*']
  s.executables  = %w[humboldt]
  s.require_paths = %w[lib]
end
