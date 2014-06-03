# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'humboldt/version'


Gem::Specification.new do |s|
  s.name        = 'humboldt'
  s.version     = Humboldt::VERSION.dup
  s.license     = 'BSD-3-Clause'
  s.platform    = 'java'
  s.authors     = ['The Burt Platform Team']
  s.email       = ['theo@burtcorp.com']
  s.homepage    = 'http://github.com/burtcorp/humboldt'
  s.summary     = %q{Tools and libraries for simplifying running Rubydoop jobs locally and on AWS Elastic MapReduce}
  s.description = %q{Humboldt provides a mapreduce API abstraction built on top of Rubydoop, and tools to run Hadoop jobs effortlessly both locally and on Amazon EMR}

  s.add_dependency 'thor'
  s.add_dependency 'rubydoop', '~> 1.1.2'
  s.add_dependency 'aws-sdk', '>= 1.16.0', '< 1.33.0'

  s.files         = Dir['lib/**/*.{rb,jar}'] + Dir['bin/*'] + Dir['config/**/*']
  s.executables  = %w[humboldt]
  s.require_paths = %w[lib]
end
