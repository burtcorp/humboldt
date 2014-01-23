# encoding: utf-8

require 'java'

begin
  IO.foreach(File.expand_path('../../.classpath', __FILE__)) { |path| $CLASSPATH << path.chomp }
  org.apache.hadoop.io.BytesWritable
rescue Errno::ENOENT, NameError => e
  if e.message['.classpath'] || e.message['org.apache.hadoop.io.BytesWritable']
    STDERR.puts e.message
    STDERR.puts
    STDERR.puts "Missing or invalid .classpath, did you run:"
    STDERR.puts
    STDERR.puts "    rake setup"
    STDERR.puts
    exit 1
  end
end

require 'bundler'; Bundler.setup(:default, :test)

require 'tmpdir'

require 'humboldt'
require 'humboldt/rspec'
require 'humboldt/emr_flow'
require 'humboldt/hadoop_status_filter'
require 'humboldt/patterns/sum_reducer'
