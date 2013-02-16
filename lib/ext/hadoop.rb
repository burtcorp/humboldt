# encoding: utf-8

module Hadoop
  module FileCache
    include_package 'org.apache.hadoop.filecache'
  end
  module Conf
    include_package 'org.apache.hadoop.conf'
  end
end
