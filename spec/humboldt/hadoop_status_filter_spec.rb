# encoding: utf-8

require_relative '../spec_helper'
require 'stringio'


module Humboldt
  describe HadoopStatusFilter do
    let :hadoop_stderr do
      StringIO.new
    end

    let :output do
      []
    end

    let :listener do
      lambda { |*args| output << args; nil }
    end

    let :filter do
      described_class.new(hadoop_stderr, listener)
    end

    describe '#run' do
      shared_examples 'output filter' do |hadoop_version|
        it 'outputs progress reports' do
          hadoop_stderr.string = counters_log
          filter.run
          output.should include([:progress, 'map 100%, reduce 100%'])
        end

        it 'outputs formatted counters' do
          hadoop_stderr.string = counters_log
          filter.run
          table = output.find { |o| o.first == :counters }
          table[1].zip(expected_counters_table).each do |actual_line, expected_line|
            actual_line.should == expected_line
          end
        end

        it 'outputs Ruby exceptions' do
          hadoop_stderr.string = ruby_error_log
          filter.run
          expected_ruby_errors.each do |line|
            output.should include([:status, line, :error])
          end
        end

        it 'outputs Ruby warnings' do
          hadoop_stderr.string = hadoop_error_log
          filter.run
          warning = output.find { |args| args[1] && args[1].include?('warning: already initialized constant ClassReader') }
          warning.should_not be_nil
          warning.first.should == :status
          warning.last.should == :warning
        end

        it 'outputs Hadoop exceptions' do
          hadoop_stderr.string = hadoop_error_log
          filter.run
          expected_hadoop_errors.each do |line|
            output.should include([:status, line, :error])
          end
        end

        it 'does not report HADOOP_HOME warnings as errors' do
          pending 'not applicable to Hadoop 2.x' if hadoop_version == '2.x'
          hadoop_stderr.string = counters_log
          filter.run
          warning = output.find { |args| args[1] && args[1].include?('$HADOOP_HOME') }
          warning.first.should eq(:stderr)
        end

        it 'does not report Hadoop deprecation warnings as errors' do
          pending 'not applicable to Hadoop 1.x' if hadoop_version == '1.x'
          hadoop_stderr.string = counters_log
          filter.run
          warning = output.find { |args| args[1] && args[1].include?('Configuration.deprecation') }
          warning.first.should eq(:stderr)
        end

        it 'does not report spurious Hadoop output as errors' do
          hadoop_stderr.string = counters_log
          filter.run
          error_statuses = output.select { |args| args[2] && args[2] == :error }
          error_statuses.find { |_, msg| msg.include?('Unable to load realm info from SCDynamicStore') }.should be_nil
          error_statuses.find { |_, msg| msg.include?('Unable to load native-hadoop library') }.should be_nil
          error_statuses.find { |_, msg| msg.include?('Snappy native library not loaded') }.should be_nil
        end
      end

      let :inline_data do
        File.readlines(__FILE__).drop_while { |line| !line.start_with?('__END') }.drop(1)
      end

      context 'when using Hadoop 1.x' do
        include_examples 'output filter', '1.x' do
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

      context 'when using Hadoop 2.x' do
        include_examples 'output filter', '2.x' do
          let :counters_log do
            lines = inline_data.drop_while { |line| !line.start_with?('%%% COUNTERS >=2.2.0 LOG') }.drop(1).take_while { |line| !line.start_with?('%%%') }
            lines.join('')
          end

          let :ruby_error_log do
            lines = inline_data.drop_while { |line| !line.start_with?('%%% 2.2.0 RUBY ERROR LOG') }.drop(1).take_while { |line| !line.start_with?('%%%') }
            lines.join('')
          end

          let :hadoop_error_log do
            lines = inline_data.drop_while { |line| !line.start_with?('%%% HADOOP 2.2.0 ERROR LOG') }.drop(1).take_while { |line| !line.start_with?('%%%') }
            lines.join('')
          end

          let :expected_counters_table do
            [
              ['File System Counters',         'FILE: Number of bytes read',            52421772],
              ['',                             'FILE: Number of bytes written',         53218751],
              ['',                             'FILE: Number of read operations',              0],
              ['',                             'FILE: Number of large read operations',        0],
              ['',                             'FILE: Number of write operations',             0],
              ['',                             '',                                            ''],
              ['Map-Reduce Framework',         'Map input records',                            1],
              ['',                             'Map output records',                           1],
              ['',                             'Map output bytes',                            28],
              ['',                             'Map output materialized bytes',               36],
              ['',                             'Input split bytes',                          151],
              ['',                             'Combine input records',                        0],
              ['',                             'Combine output records',                       0],
              ['',                             'Reduce input groups',                          1],
              ['',                             'Reduce shuffle bytes',                         0],
              ['',                             'Reduce input records',                         1],
              ['',                             'Reduce output records',                        2],
              ['',                             'Spilled Records',                              2],
              ['',                             'Shuffled Maps',                                0],
              ['',                             'Failed Shuffles',                              0],
              ['',                             'Merged Map outputs',                           0],
              ['',                             'GC time elapsed (ms)',                       133],
              ['',                             'Total committed heap usage (bytes)',   291323904],
              ['',                             '',                                            ''],
              ['File Input Format Counters',   'Bytes Read',                                   7],
              ['',                             '',                                            ''],
              ['File Output Format Counters',  'Bytes Written',                               69],
            ]
          end
        end

        let :expected_ruby_errors do
          [
            %{NoMethodError: undefined method `foo' for nil:NilClass},
            %{         Mapper at /tmp/hadoop-bjorne/hadoop-unjar1569962793928058865/combined_text_test.rb:11},
            %{  instance_exec at org/jruby/RubyBasicObject.java:1565},
            %{            map at /tmp/hadoop-bjorne/hadoop-unjar1569962793928058865/gems/humboldt-0.5.0-java/lib/humboldt/mapper.rb:10},
          ]
        end

        let :expected_hadoop_errors do
          [
            'Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory file:/Users/bjorne/Code/humboldt/spec/fixtures/test_project/data/test_project/output/cache already exists',
            '	at org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.checkOutputSpecs(FileOutputFormat.java:146)',
            '	at org.apache.hadoop.mapreduce.JobSubmitter.checkSpecs(JobSubmitter.java:456)',
          ]
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

%%% COUNTERS >=2.2.0 LOG %%%

2014-05-30 12:45:13.268 java[11527:1703] Unable to load realm info from SCDynamicStore
14/05/30 12:45:13 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
14/05/30 12:45:13 INFO Configuration.deprecation: mapred.job.tracker is deprecated. Instead, use mapreduce.jobtracker.address
Warning! Using `format: :combined_text` will not work with remote input paths (e.g. S3) and Hadoop 1.x. Cf. https://issues.apache.org/jira/browse/MAPREDUCE-1806
14/05/30 12:45:14 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
14/05/30 12:45:14 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
14/05/30 12:45:14 INFO input.FileInputFormat: Total input paths to process : 1
14/05/30 12:45:14 INFO mapreduce.JobSubmitter: number of splits:1
14/05/30 12:45:14 INFO Configuration.deprecation: user.name is deprecated. Instead, use mapreduce.job.user.name
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.cache.files.filesizes is deprecated. Instead, use mapreduce.job.cache.files.filesizes
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.cache.files is deprecated. Instead, use mapreduce.job.cache.files
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.output.value.class is deprecated. Instead, use mapreduce.job.output.value.class
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.mapoutput.value.class is deprecated. Instead, use mapreduce.map.output.value.class
14/05/30 12:45:14 INFO Configuration.deprecation: mapreduce.map.class is deprecated. Instead, use mapreduce.job.map.class
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.job.name is deprecated. Instead, use mapreduce.job.name
14/05/30 12:45:14 INFO Configuration.deprecation: mapreduce.reduce.class is deprecated. Instead, use mapreduce.job.reduce.class
14/05/30 12:45:14 INFO Configuration.deprecation: mapreduce.inputformat.class is deprecated. Instead, use mapreduce.job.inputformat.class
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.input.dir is deprecated. Instead, use mapreduce.input.fileinputformat.inputdir
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.output.dir is deprecated. Instead, use mapreduce.output.fileoutputformat.outputdir
14/05/30 12:45:14 INFO Configuration.deprecation: mapreduce.outputformat.class is deprecated. Instead, use mapreduce.job.outputformat.class
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.cache.files.timestamps is deprecated. Instead, use mapreduce.job.cache.files.timestamps
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.output.key.class is deprecated. Instead, use mapreduce.job.output.key.class
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.mapoutput.key.class is deprecated. Instead, use mapreduce.map.output.key.class
14/05/30 12:45:14 INFO Configuration.deprecation: mapred.working.dir is deprecated. Instead, use mapreduce.job.working.dir
14/05/30 12:45:15 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_local1827895999_0001
14/05/30 12:45:15 WARN conf.Configuration: file:/tmp/hadoop-bjorne/mapred/staging/bjorne1827895999/.staging/job_local1827895999_0001/job.xml:an attempt to override final parameter: mapreduce.job.end-notification.max.retry.interval;  Ignoring.
14/05/30 12:45:15 WARN conf.Configuration: file:/tmp/hadoop-bjorne/mapred/staging/bjorne1827895999/.staging/job_local1827895999_0001/job.xml:an attempt to override final parameter: mapreduce.job.end-notification.max.attempts;  Ignoring.
14/05/30 12:45:15 INFO mapred.LocalDistributedCacheManager: Creating symlink: /tmp/hadoop-bjorne/mapred/local/1401446715328/super_important_file.doc <- /Users/bjorne/Code/humboldt/spec/fixtures/test_project/important.doc
14/05/30 12:45:15 INFO mapred.LocalDistributedCacheManager: Localized file:/Users/bjorne/Code/humboldt/spec/fixtures/test_project/cached_files/super_important_file.doc as file:/tmp/hadoop-bjorne/mapred/local/1401446715328/super_important_file.doc
14/05/30 12:45:15 INFO mapred.LocalDistributedCacheManager: Creating symlink: /tmp/hadoop-bjorne/mapred/local/1401446715329/another_file <- /Users/bjorne/Code/humboldt/spec/fixtures/test_project/another_file
14/05/30 12:45:15 INFO mapred.LocalDistributedCacheManager: Localized file:/Users/bjorne/Code/humboldt/spec/fixtures/test_project/cached_files/another_file as file:/tmp/hadoop-bjorne/mapred/local/1401446715329/another_file
14/05/30 12:45:15 INFO Configuration.deprecation: mapred.cache.localFiles is deprecated. Instead, use mapreduce.job.cache.local.files
14/05/30 12:45:16 WARN conf.Configuration: file:/tmp/hadoop-bjorne/mapred/local/localRunner/bjorne/job_local1827895999_0001/job_local1827895999_0001.xml:an attempt to override final parameter: mapreduce.job.end-notification.max.retry.interval;  Ignoring.
14/05/30 12:45:16 WARN conf.Configuration: file:/tmp/hadoop-bjorne/mapred/local/localRunner/bjorne/job_local1827895999_0001/job_local1827895999_0001.xml:an attempt to override final parameter: mapreduce.job.end-notification.max.attempts;  Ignoring.
14/05/30 12:45:16 INFO mapreduce.Job: The url to track the job: http://localhost:8080/
14/05/30 12:45:16 INFO mapreduce.Job: Running job: job_local1827895999_0001
14/05/30 12:45:16 INFO mapred.LocalJobRunner: OutputCommitter set in config null
14/05/30 12:45:16 INFO mapred.LocalJobRunner: OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
14/05/30 12:45:16 INFO mapred.LocalJobRunner: Waiting for map tasks
14/05/30 12:45:16 INFO mapred.LocalJobRunner: Starting task: attempt_local1827895999_0001_m_000000_0
14/05/30 12:45:16 INFO util.ProcfsBasedProcessTree: ProcfsBasedProcessTree currently is supported only on Linux.
14/05/30 12:45:16 INFO mapred.Task:  Using ResourceCalculatorProcessTree : null
14/05/30 12:45:16 INFO mapred.MapTask: Processing split: file:/Users/bjorne/Code/humboldt/spec/fixtures/test_project/data/input/cache/input.txt:0+7
14/05/30 12:45:16 INFO mapred.MapTask: Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
14/05/30 12:45:16 INFO mapred.MapTask: (EQUATOR) 0 kvi 26214396(104857584)
14/05/30 12:45:16 INFO mapred.MapTask: mapreduce.task.io.sort.mb: 100
14/05/30 12:45:16 INFO mapred.MapTask: soft limit at 83886080
14/05/30 12:45:16 INFO mapred.MapTask: bufstart = 0; bufvoid = 104857600
14/05/30 12:45:16 INFO mapred.MapTask: kvstart = 26214396; length = 6553600
14/05/30 12:45:16 INFO mapred.LocalJobRunner:
14/05/30 12:45:16 INFO mapred.MapTask: Starting flush of map output
14/05/30 12:45:16 INFO mapred.MapTask: Spilling map output
14/05/30 12:45:16 INFO mapred.MapTask: bufstart = 0; bufend = 28; bufvoid = 104857600
14/05/30 12:45:16 INFO mapred.MapTask: kvstart = 26214396(104857584); kvend = 26214396(104857584); length = 1/6553600
14/05/30 12:45:16 INFO mapred.MapTask: Finished spill 0
14/05/30 12:45:16 INFO mapred.Task: Task:attempt_local1827895999_0001_m_000000_0 is done. And is in the process of committing
14/05/30 12:45:16 INFO mapred.LocalJobRunner: map
14/05/30 12:45:16 INFO mapred.Task: Task 'attempt_local1827895999_0001_m_000000_0' done.
14/05/30 12:45:16 INFO mapred.LocalJobRunner: Finishing task: attempt_local1827895999_0001_m_000000_0
14/05/30 12:45:16 INFO mapred.LocalJobRunner: Map task executor complete.
14/05/30 12:45:16 INFO util.ProcfsBasedProcessTree: ProcfsBasedProcessTree currently is supported only on Linux.
14/05/30 12:45:16 INFO mapred.Task:  Using ResourceCalculatorProcessTree : null
14/05/30 12:45:16 INFO mapred.Merger: Merging 1 sorted segments
14/05/30 12:45:16 INFO mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 23 bytes
14/05/30 12:45:16 INFO mapred.LocalJobRunner:
14/05/30 12:45:16 INFO Configuration.deprecation: mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
14/05/30 12:45:16 INFO mapred.Task: Task:attempt_local1827895999_0001_r_000000_0 is done. And is in the process of committing
14/05/30 12:45:16 INFO mapred.LocalJobRunner:
14/05/30 12:45:16 INFO mapred.Task: Task attempt_local1827895999_0001_r_000000_0 is allowed to commit now
14/05/30 12:45:16 INFO output.FileOutputCommitter: Saved output of task 'attempt_local1827895999_0001_r_000000_0' to file:/Users/bjorne/Code/humboldt/spec/fixtures/test_project/data/test_project/output/cache/_temporary/0/task_local1827895999_0001_r_000000
14/05/30 12:45:16 INFO mapred.LocalJobRunner: reduce > reduce
14/05/30 12:45:16 INFO mapred.Task: Task 'attempt_local1827895999_0001_r_000000_0' done.
14/05/30 12:45:17 INFO mapreduce.Job: Job job_local1827895999_0001 running in uber mode : false
14/05/30 12:45:17 INFO mapreduce.Job:  map 100% reduce 100%
15/10/01 21:13:23 INFO compress.CodecPool: Got brand-new decompressor [.gz]
15/10/01 21:24:14 INFO reduce.LocalFetcher: localfetcher#1 about to shuffle output of map attempt_local1275396716_0001_m_000127_0 decomp: 6285685 len: 6285689 to MEMORY
15/10/01 21:24:14 INFO reduce.InMemoryMapOutput: Read 6285685 bytes from map-output for attempt_local1275396716_0001_m_000127_0
14/05/30 12:45:17 INFO mapreduce.Job: Job job_local1827895999_0001 completed successfully
14/05/30 12:45:17 INFO mapreduce.Job: Counters: 24
	File System Counters
		FILE: Number of bytes read=52421772
		FILE: Number of bytes written=53218751
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
	Map-Reduce Framework
		Map input records=1
		Map output records=1
		Map output bytes=28
		Map output materialized bytes=36
		Input split bytes=151
		Combine input records=0
		Combine output records=0
		Reduce input groups=1
		Reduce shuffle bytes=0
		Reduce input records=1
		Reduce output records=2
		Spilled Records=2
		Shuffled Maps =0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=133
		Total committed heap usage (bytes)=291323904
	File Input Format Counters
		Bytes Read=7
	File Output Format Counters
		Bytes Written=69

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

%%% 2.2.0 RUBY ERROR LOG %%%

14/05/30 14:04:35 INFO mapred.MapTask: kvstart = 26214396; length = 6553600
NoMethodError: undefined method `foo' for nil:NilClass
         Mapper at /tmp/hadoop-bjorne/hadoop-unjar1569962793928058865/combined_text_test.rb:11
  instance_exec at org/jruby/RubyBasicObject.java:1565
            map at /tmp/hadoop-bjorne/hadoop-unjar1569962793928058865/gems/humboldt-0.5.0-java/lib/humboldt/mapper.rb:10
14/05/30 14:04:35 INFO mapred.MapTask: Starting flush of map output
14/05/30 14:04:35 INFO mapred.LocalJobRunner: Map task executor complete.
14/05/30 14:04:35 WARN mapred.LocalJobRunner: job_local1686491183_0001
java.lang.Exception: org.jruby.embed.InvokeFailedException: (NoMethodError) undefined method `foo' for nil:NilClass
	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:403)
Caused by: org.jruby.embed.InvokeFailedException: (NoMethodError) undefined method `foo' for nil:NilClass
	at org.jruby.embed.internal.EmbedRubyObjectAdapterImpl.call(EmbedRubyObjectAdapterImpl.java:317)
	at org.jruby.embed.internal.EmbedRubyObjectAdapterImpl.callMethod(EmbedRubyObjectAdapterImpl.java:249)
	at org.jruby.embed.ScriptingContainer.callMethod(ScriptingContainer.java:1369)
	at rubydoop.InstanceContainer.callMethod(InstanceContainer.java:68)
	at rubydoop.MapperProxy.map(MapperProxy.java:13)
	at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:145)
	at rubydoop.MapperProxy.run(MapperProxy.java:17)
	at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:763)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:339)
	at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.run(LocalJobRunner.java:235)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)
	at java.util.concurrent.FutureTask$Sync.innerRun(FutureTask.java:334)
	at java.util.concurrent.FutureTask.run(FutureTask.java:166)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:724)
Caused by: org.jruby.exceptions.RaiseException: (NoMethodError) undefined method `foo' for nil:NilClass
	at RUBY.Mapper(/tmp/hadoop-bjorne/hadoop-unjar1569962793928058865/combined_text_test.rb:11)
	at org.jruby.RubyBasicObject.instance_exec(org/jruby/RubyBasicObject.java:1565)
	at RUBY.map(/tmp/hadoop-bjorne/hadoop-unjar1569962793928058865/gems/humboldt-0.5.0-java/lib/humboldt/mapper.rb:10)
14/05/30 14:04:36 INFO mapreduce.Job: Job job_local1686491183_0001 running in uber mode : false
14/05/30 14:04:36 INFO mapreduce.Job:  map 0% reduce 0%
14/05/30 14:04:36 INFO mapreduce.Job: Job job_local1686491183_0001 failed with state FAILED due to: NA
14/05/30 14:04:36 INFO mapreduce.Job: Counters: 0

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

%%% HADOOP 2.2.0 ERROR LOG %%%

file:/tmp/hadoop-theo/hadoop-unjar4710419695108193976/lib/jruby-complete-1.7.0.RC1.jar!/jruby/jruby.rb:11 warning: already initialized constant ClassReader
2014-05-30 14:00:38.293 java[20314:1703] Unable to load realm info from SCDynamicStore
14/05/30 14:00:38 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
14/05/30 14:00:38 INFO Configuration.deprecation: mapred.job.tracker is deprecated. Instead, use mapreduce.jobtracker.address
Warning! Using `format: :combined_text` will not work with remote input paths (e.g. S3) and Hadoop 1.x. Cf. https://issues.apache.org/jira/browse/MAPREDUCE-1806
14/05/30 14:00:38 INFO Configuration.deprecation: session.id is deprecated. Instead, use dfs.metrics.session-id
14/05/30 14:00:38 INFO jvm.JvmMetrics: Initializing JVM Metrics with processName=JobTracker, sessionId=
14/05/30 14:00:38 ERROR security.UserGroupInformation: PriviledgedActionException as:bjorne (auth:SIMPLE) cause:org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory file:/Users/bjorne/Code/humboldt/spec/fixtures/test_project/data/test_project/output/cache already exists
Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory file:/Users/bjorne/Code/humboldt/spec/fixtures/test_project/data/test_project/output/cache already exists
	at org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.checkOutputSpecs(FileOutputFormat.java:146)
	at org.apache.hadoop.mapreduce.JobSubmitter.checkSpecs(JobSubmitter.java:456)
	at org.apache.hadoop.mapreduce.JobSubmitter.submitJobInternal(JobSubmitter.java:342)
	at org.apache.hadoop.mapreduce.Job$10.run(Job.java:1268)
	at org.apache.hadoop.mapreduce.Job$10.run(Job.java:1265)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:415)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1491)
	at org.apache.hadoop.mapreduce.Job.submit(Job.java:1265)
	at org.apache.hadoop.mapreduce.Job.waitForCompletion(Job.java:1286)
	at rubydoop.RubydoopJobRunner.run(RubydoopJobRunner.java:29)
	at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:70)
	at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:84)
	at rubydoop.RubydoopJobRunner.main(RubydoopJobRunner.java:74)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at org.apache.hadoop.util.RunJar.main(RunJar.java:212)
