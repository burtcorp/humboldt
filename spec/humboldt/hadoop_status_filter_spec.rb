# encoding: utf-8

require_relative '../spec_helper'
require 'stringio'


module Humboldt
  class FakeShell
    attr_reader :messages, :statuses, :tables

    def initialize
      @messages = []
      @statuses = []
      @tables = []
    end

    def say(message='')
      @messages << message
    end

    def say_status(status, message, color=nil)
      @statuses << [status, message, color]
    end

    def print_table(array)
      @tables << array
    end
  end

  describe HadoopStatusFilter do
    let :hadoop_stderr do
      StringIO.new
    end

    let :shell do
      FakeShell.new
    end

    let :filter do
      described_class.new(hadoop_stderr, shell, true)
    end

    describe '#run' do
      shared_examples 'output filter' do
        it 'outputs progress reports' do
          hadoop_stderr.string = counters_log
          filter.run
          shell.statuses.should include([:progress, 'map 100%, reduce 100%', nil])
        end

        it 'outputs formatted counters' do
          hadoop_stderr.string = counters_log
          filter.run
          shell.tables.first.zip(expected_counters_table).each do |actual_line, expected_line|
            actual_line.should == expected_line
          end
          shell.tables.first.should == expected_counters_table
        end

        it 'outputs Ruby exceptions' do
          hadoop_stderr.string = ruby_error_log
          filter.run
          expected_ruby_errors.each do |line|
            shell.statuses.should include([:error, line, :red])
          end
        end

        it 'outputs Ruby warnings' do
          hadoop_stderr.string = hadoop_error_log
          filter.run
          warning = shell.statuses.find { |_, msg, _| msg.include?('warning: already initialized constant ClassReader') }
          warning.should_not be_nil
          warning.first.should == :warning
          warning.last.should == :yellow
        end

        it 'outputs Hadoop exceptions' do
          hadoop_stderr.string = hadoop_error_log
          filter.run
          expected_hadoop_errors.each do |line|
            shell.statuses.should include([:error, line, :red])
          end
        end

        it 'does not report HADOOP_HOME warnings as errors' do
          hadoop_stderr.string = counters_log
          filter.run
          shell.statuses.find { |_, msg, _| msg.include?('$HADOOP_HOME') }.should be_nil
        end

        it 'does not report spurious Hadoop output as errors' do
          hadoop_stderr.string = counters_log
          filter.run
          error_statuses = shell.statuses.select { |status, _, _| status == :error }
          error_statuses.find { |_, msg, _| msg.include?('Unable to load realm info from SCDynamicStore') }.should be_nil
          error_statuses.find { |_, msg, _| msg.include?('Unable to load native-hadoop library') }.should be_nil
          error_statuses.find { |_, msg, _| msg.include?('Snappy native library not loaded') }.should be_nil
        end
      end

      let :inline_data do
        File.readlines(__FILE__).drop_while { |line| !line.start_with?('__END') }.drop(1)
      end

      context 'hadoop 1.0.3' do
        it_behaves_like 'output filter' do
          let :counters_log do
            lines = inline_data.drop_while { |line| !line.start_with?('%%% COUNTERS LOG') }.drop(1).take_while { |line| !line.start_with?('%%%') }
            lines.join('')
          end


          let :ruby_error_log do
            lines = inline_data.drop_while { |line| !line.start_with?('%%% RUBY ERROR LOG') }.drop(1).take_while { |line| !line.start_with?('%%%') }
            lines.join('')
          end

          let :hadoop_error_log do
            lines = inline_data.drop_while { |line| !line.start_with?('%%% HADOOP ERROR LOG') }.drop(1).take_while { |line| !line.start_with?('%%%') }
            lines.join('')
          end

          let :expected_counters_table do
            [
              ['Rubydoop',                    'JRuby runtimes created',                     2],
              ['',                            '',                                          ''],
              ['File Output Format Counters', 'Bytes Written',                              8],
              ['',                            '',                                          ''],
              ['FileSystemCounters',          'FILE_BYTES_READ',                     46115331],
              ['',                            'FILE_BYTES_WRITTEN',                  46526366],
              ['',                            '',                                          ''],
              ['File Input Format Counters',  'Bytes Read',                            214234],
              ['',                            '',                                          ''],
              ['Map-Reduce Framework',        'Map output materialized bytes',         417723],
              ['',                            'Map input records',                        338],
              ['',                            'Reduce shuffle bytes',                       0],
              ['',                            'Spilled Records',                          676],
              ['',                            'Map output bytes',                      416365],
              ['',                            'Total committed heap usage (bytes)', 581795840],
              ['',                            'SPLIT_RAW_BYTES',                          238],
              ['',                            'Combine input records',                      0],
              ['',                            'Reduce input records',                     338],
              ['',                            'Reduce input groups',                      338],
              ['',                            'Combine output records',                     0],
              ['',                            'Reduce output records',                      0],
              ['',                            'Map output records',                       338]
            ]
          end

          let :expected_ruby_errors do
            [
              %{org.jruby.exceptions.RaiseException: (NoMethodError) undefined method `asdas' for #<DuplicatesFinder::JoinMultipleInputs:0x5d511019>},
              %{  at RUBY.reduce(/tmp/hadoop-theo/hadoop-unjar8455542447103659575/duplicates_finder.rb:34)},
            ]
          end

          let :expected_hadoop_errors do
            [
              'java.lang.NullPointerException',
              '  at org.apache.hadoop.mapred.MapTask$MapOutputBuffer.collect(MapTask.java:1018)',
              '  at org.apache.hadoop.mapred.MapTask$NewOutputCollector.write(MapTask.java:691)',
              '  at org.apache.hadoop.mapreduce.TaskInputOutputContext.write(TaskInputOutputContext.java:80)',
              '  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)',
            ]
          end
        end
      end



      end
    end
  end
end

__END__

%%% COUNTERS LOG %%%

Warning: $HADOOP_HOME is deprecated.

2012-10-04 15:59:00.156 java[7300:1703] Unable to load realm info from SCDynamicStore
12/10/04 15:59:00 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
12/10/04 15:59:00 INFO input.FileInputFormat: Total input paths to process : 1
12/10/04 15:59:00 WARN snappy.LoadSnappy: Snappy native library not loaded
12/10/04 15:59:00 INFO mapred.JobClient: Running job: job_local_0001
12/10/04 15:59:01 INFO mapred.Task:  Using ResourceCalculatorPlugin : null
12/10/04 15:59:01 INFO mapred.MapTask: io.sort.mb = 100
12/10/04 15:59:01 INFO mapred.MapTask: data buffer = 79691776/99614720
12/10/04 15:59:01 INFO mapred.MapTask: record buffer = 262144/327680
file:/tmp/hadoop-theo/hadoop-unjar6889015609914014281/lib/jruby-complete-1.7.0.RC1.jar!/jruby/jruby.rb:11 warning: already initialized constant ClassReader
12/10/04 15:59:01 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
12/10/04 15:59:01 INFO mapred.JobClient:  map 0% reduce 0%
12/10/04 15:59:03 INFO mapred.MapTask: Starting flush of map output
12/10/04 15:59:03 INFO mapred.MapTask: Finished spill 0
12/10/04 15:59:03 INFO mapred.Task: Task:attempt_local_0001_m_000000_0 is done. And is in the process of commiting
12/10/04 15:59:04 INFO mapred.LocalJobRunner: false
12/10/04 15:59:04 INFO mapred.Task: Task 'attempt_local_0001_m_000000_0' done.
12/10/04 15:59:04 INFO mapred.Task:  Using ResourceCalculatorPlugin : null
12/10/04 15:59:04 INFO mapred.LocalJobRunner: false
12/10/04 15:59:04 INFO mapred.Merger: Merging 1 sorted segments
12/10/04 15:59:04 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 417719 bytes
12/10/04 15:59:04 INFO mapred.LocalJobRunner: false
12/10/04 15:59:04 INFO mapred.JobClient:  map 100% reduce 0%
12/10/04 15:59:05 INFO mapred.Task: Task:attempt_local_0001_r_000000_0 is done. And is in the process of commiting
12/10/04 15:59:05 INFO mapred.LocalJobRunner: false
12/10/04 15:59:05 INFO mapred.Task: Task attempt_local_0001_r_000000_0 is allowed to commit now
12/10/04 15:59:05 INFO output.FileOutputCommitter: Saved output of task 'attempt_local_0001_r_000000_0' to data/duplicates_finder/output
12/10/04 15:59:07 INFO mapred.LocalJobRunner: reduce > reduce
12/10/04 15:59:07 INFO mapred.Task: Task 'attempt_local_0001_r_000000_0' done.
12/10/04 15:59:07 INFO mapred.JobClient:  map 100% reduce 100%
12/10/04 15:59:07 INFO mapred.JobClient: Job complete: job_local_0001
12/10/04 15:59:07 INFO mapred.JobClient: Counters: 18
12/10/04 15:59:07 INFO mapred.JobClient:   Rubydoop
12/10/04 15:59:07 INFO mapred.JobClient:     JRuby runtimes created=2
12/10/04 15:59:07 INFO mapred.JobClient:   File Output Format Counters 
12/10/04 15:59:07 INFO mapred.JobClient:     Bytes Written=8
12/10/04 15:59:07 INFO mapred.JobClient:   FileSystemCounters
12/10/04 15:59:07 INFO mapred.JobClient:     FILE_BYTES_READ=46115331
12/10/04 15:59:07 INFO mapred.JobClient:     FILE_BYTES_WRITTEN=46526366
12/10/04 15:59:07 INFO mapred.JobClient:   File Input Format Counters 
12/10/04 15:59:07 INFO mapred.JobClient:     Bytes Read=214234
12/10/04 15:59:07 INFO mapred.JobClient:   Map-Reduce Framework
12/10/04 15:59:07 INFO mapred.JobClient:     Map output materialized bytes=417723
12/10/04 15:59:07 INFO mapred.JobClient:     Map input records=338
12/10/04 15:59:07 INFO mapred.JobClient:     Reduce shuffle bytes=0
12/10/04 15:59:07 INFO mapred.JobClient:     Spilled Records=676
12/10/04 15:59:07 INFO mapred.JobClient:     Map output bytes=416365
12/10/04 15:59:07 INFO mapred.JobClient:     Total committed heap usage (bytes)=581795840
12/10/04 15:59:07 INFO mapred.JobClient:     SPLIT_RAW_BYTES=238
12/10/04 15:59:07 INFO mapred.JobClient:     Combine input records=0
12/10/04 15:59:07 INFO mapred.JobClient:     Reduce input records=338
12/10/04 15:59:07 INFO mapred.JobClient:     Reduce input groups=338
12/10/04 15:59:07 INFO mapred.JobClient:     Combine output records=0
12/10/04 15:59:07 INFO mapred.JobClient:     Reduce output records=0
12/10/04 15:59:07 INFO mapred.JobClient:     Map output records=338

%%% RUBY ERROR LOG %%%

12/10/04 16:35:56 INFO mapred.JobClient:  map 100% reduce 0%
12/10/04 16:35:56 WARN mapred.LocalJobRunner: job_local_0001
org.jruby.exceptions.RaiseException: (NoMethodError) undefined method `asdas' for #<DuplicatesFinder::JoinMultipleInputs:0x5d511019>
  at RUBY.reduce(/tmp/hadoop-theo/hadoop-unjar8455542447103659575/duplicates_finder.rb:34)
12/10/04 16:35:57 INFO mapred.JobClient: Job complete: job_local_0001
12/10/04 16:35:57 INFO mapred.JobClient: Counters: 17
12/10/04 16:35:57 INFO mapred.JobClient:   Rubydoop
12/10/04 16:35:57 INFO mapred.JobClient:     JRuby runtimes created=1
12/10/04 16:35:57 INFO mapred.JobClient:   FileSystemCounters
12/10/04 16:35:57 INFO mapred.JobClient:     FILE_BYTES_READ=22849005
12/10/04 16:35:57 INFO mapred.JobClient:     FILE_BYTES_WRITTEN=23263380
12/10/04 16:35:57 INFO mapred.JobClient:   File Input Format Counters 12/10/04 16:35:57 INFO mapred.JobClient:     Bytes Read=214234
12/10/04 16:35:57 INFO mapred.JobClient:   Map-Reduce Framework
12/10/04 16:35:57 INFO mapred.JobClient:     Map output materialized bytes=417723
12/10/04 16:35:57 INFO mapred.JobClient:     Map input records=338
12/10/04 16:35:57 INFO mapred.JobClient:     Reduce shuffle bytes=0
12/10/04 16:35:57 INFO mapred.JobClient:     Spilled Records=338
12/10/04 16:35:57 INFO mapred.JobClient:     Map output bytes=416365
12/10/04 16:35:57 INFO mapred.JobClient:     Total committed heap usage (bytes)=319619072
12/10/04 16:35:57 INFO mapred.JobClient:     SPLIT_RAW_BYTES=238
12/10/04 16:35:57 INFO mapred.JobClient:     Combine input records=0
12/10/04 16:35:57 INFO mapred.JobClient:     Reduce input records=0
12/10/04 16:35:57 INFO mapred.JobClient:     Reduce input groups=0
12/10/04 16:35:57 INFO mapred.JobClient:     Combine output records=0
12/10/04 16:35:57 INFO mapred.JobClient:     Reduce output records=0
12/10/04 16:35:57 INFO mapred.JobClient:     Map output records=338

%%% HADOOP ERROR LOG %%%

12/10/04 17:04:52 INFO mapred.JobClient: Running job: job_local_0001
12/10/04 17:04:52 INFO mapred.Task:  Using ResourceCalculatorPlugin : null
12/10/04 17:04:52 INFO mapred.MapTask: io.sort.mb = 100
12/10/04 17:04:52 INFO mapred.MapTask: data buffer = 79691776/99614720
12/10/04 17:04:52 INFO mapred.MapTask: record buffer = 262144/327680
file:/tmp/hadoop-theo/hadoop-unjar4710419695108193976/lib/jruby-complete-1.7.0.RC1.jar!/jruby/jruby.rb:11 warning: already initialized constant ClassReader
12/10/04 17:04:53 INFO mapred.JobClient:  map 0% reduce 0%
12/10/04 17:04:54 WARN mapred.LocalJobRunner: job_local_0001
java.lang.NullPointerException
  at org.apache.hadoop.mapred.MapTask$MapOutputBuffer.collect(MapTask.java:1018)
  at org.apache.hadoop.mapred.MapTask$NewOutputCollector.write(MapTask.java:691)
  at org.apache.hadoop.mapreduce.TaskInputOutputContext.write(TaskInputOutputContext.java:80)
  at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
  at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  at java.lang.reflect.Method.invoke(Method.java:601)
  at org.jruby.javasupport.JavaMethod.invokeDirectWithExceptionHandling(JavaMethod.java:470)
  at org.jruby.javasupport.JavaMethod.invokeDirect(JavaMethod.java:328)
  at org.jruby.java.invokers.InstanceMethodInvoker.call(InstanceMethodInvoker.java:71)
  at org.jruby.runtime.callsite.CachingCallSite.cacheAndCall(CachingCallSite.java:346)
  at org.jruby.runtime.callsite.CachingCallSite.call(CachingCallSite.java:204)
  at org.jruby.ast.CallTwoArgNode.interpret(CallTwoArgNode.java:59)
  at org.jruby.ast.NewlineNode.interpret(NewlineNode.java:105)
  at org.jruby.ast.BlockNode.interpret(BlockNode.java:71)
  at org.jruby.ast.IfNode.interpret(IfNode.java:116)
  at org.jruby.ast.NewlineNode.interpret(NewlineNode.java:105)
  at org.jruby.ast.BlockNode.interpret(BlockNode.java:71)
  at org.jruby.evaluator.ASTInterpreter.INTERPRET_METHOD(ASTInterpreter.java:75)
  at org.jruby.internal.runtime.methods.InterpretedMethod.call(InterpretedMethod.java:112)
  at org.jruby.internal.runtime.methods.InterpretedMethod.call(InterpretedMethod.java:126)
  at org.jruby.internal.runtime.methods.DefaultMethod.call(DefaultMethod.java:167)
  at org.jruby.RubyClass.finvoke(RubyClass.java:734)
  at org.jruby.javasupport.util.RuntimeHelpers.invoke(RuntimeHelpers.java:532)
  at org.jruby.RubyBasicObject.callMethod(RubyBasicObject.java:369)
  at rubydoop.InstanceContainer.callMethod(InstanceContainer.java:82)
  at rubydoop.MapperProxy.map(MapperProxy.java:15)
  at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:144)
  at rubydoop.MapperProxy.run(MapperProxy.java:19)
  at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:764)
  at org.apache.hadoop.mapred.MapTask.run(MapTask.java:370)
  at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:212)
12/10/04 17:04:54 INFO mapred.JobClient: Job complete: job_local_0001
12/10/04 17:04:54 INFO mapred.JobClient: Counters: 0
