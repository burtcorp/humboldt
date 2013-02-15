# encoding: utf-8

require 'java'

IO.foreach(File.expand_path('../../.classpath', __FILE__)) { |path| $CLASSPATH << path.chomp }

require 'bundler'; Bundler.setup(:default, :test)

require 'tmpdir'

require 'humboldt'
require 'humboldt/rspec'
require 'humboldt/emr_flow'
require 'humboldt/hadoop_status_filter'
require 'humboldt/patterns/sum_reducer'
