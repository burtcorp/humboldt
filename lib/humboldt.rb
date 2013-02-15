# encoding: utf-8

require 'rubydoop'
require 'hadoop'

module Hadoop
  module FileCache
    include_package 'org.apache.hadoop.filecache'
  end
  module Conf
    include_package 'org.apache.hadoop.conf'
  end
end

require 'humboldt/type_converters'
require 'humboldt/processor'
require 'humboldt/mapper'
require 'humboldt/reducer'
require 'humboldt/prefix_grouping'
