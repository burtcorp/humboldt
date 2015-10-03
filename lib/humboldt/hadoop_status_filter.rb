# encoding: utf-8

require 'open3'

module Humboldt
  # @private
  class HadoopStatusFilter
    def initialize(hadoop_stderr, listener)
      @hadoop_stderr = hadoop_stderr
      @listener = listener
      @counters = {}
    end

    def self.run_command_with_filtering(*args, &listener)
      Open3.popen3(*args) do |stdin, stdout, stderr, wait_thr|
        stdin.close
        stdout_printer = Thread.new(stdout) do |stdout|
          while line = stdout.gets
            listener.call(:stdout, line.chomp)
          end
        end
        stderr_printer = Thread.new(stderr) do |stderr|
          filter = new(stderr, listener)
          filter.run
        end
        stdout_printer.join
        stderr_printer.join
        if wait_thr.value.exitstatus == 0
          listener.call(:done)
        else
          listener.call(:failed)
        end
      end
    end

    def run
      counter_group = nil
      while line = @hadoop_stderr.gets
        if @counters_printing && (hadoop_log?(line) || line =~ /^\t+/)
          case line.chomp
          when /(?:JobClient:     |\t+)([^\t]+)=(\d+)$/
            if counter_group
              @counters[counter_group] ||= {}
              @counters[counter_group][$1.strip] = $2.to_i
            end
          when /(?:JobClient:   |\t+)([^\t]+)$/
            counter_group = $1.strip
          end
        elsif @error_printing && !hadoop_log?(line) && !ignore?(line)
          report_error(line)
        elsif ignore?(line)
          # do nothing
        else
          @counters_printing = false
          @error_printing = false
          case line
          when /map (\d+)% reduce (\d+)%/
            report_progress($1, $2)
          when /Counters: \d+/
            @counters_printing = true
          else
            unless hadoop_log?(line)
              @error_printing = true
              if line =~ /warning(!|:)/i
                @error_type = :warning
              else
                @error_type = :error
              end
              report_error(line)
            end
          end
        end
        @listener.call(:stderr, line.chomp)
      end
      print_counters_table
    end

    private

    def hadoop_log?(line)
      line =~ /(?:INFO|WARN) (?:mapred|input|output|util|jvm|mapreduce|compress|reduce)\./
    end

    def ignore?(line)
      case line
      when /^\s*$/, 
           /Warning: \$HADOOP_HOME is deprecated/, 
           /Unable to load realm info from SCDynamicStore/,
           /Unable to load native-hadoop library/,
           /Snappy native library not loaded/,
           /Configuration.deprecation:/,
           /WARN conf.Configuration.*attempt to override final parameter.*ignoring/i
        true
      else
        false
      end
    end

    def report_progress(map, reduce)
      @listener.call(:progress, "map #{map}%, reduce #{reduce}%")
    end

    def report_error(line)
      @listener.call(:status, line.chomp, @error_type)
    end

    def print_counters_table
      table = @counters.flat_map do |group, counters|
        [
          [group, *counters.first],
          *counters.drop(1).map { |counter, value| ['', counter, value] },
          ['', '', '']
        ]
      end
      table.pop
      @listener.call(:counters, table)
    end
  end
end
