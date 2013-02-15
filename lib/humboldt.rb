# encoding: utf-8

require 'rubydoop'
require 'hadoop'

module Hadoop
  module FileCache
    java_import 'org.apache.hadoop.filecache.DistributedCache'
  end
end

require 'humboldt/type_converters'
require 'humboldt/processor'
require 'humboldt/mapper'
require 'humboldt/reducer'
require 'humboldt/prefix_grouping'
