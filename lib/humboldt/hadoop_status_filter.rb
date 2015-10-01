# encoding: utf-8

module Humboldt
  class HadoopStatusFilter
    def initialize(hadoop_stderr, shell, silent)
      @hadoop_stderr = hadoop_stderr
      @shell = shell
      @silent = silent
      @counters = {}
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
        @shell.say(line.chomp, :red) unless @silent
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
      @shell.say_status(:progress, "map #{map}%, reduce #{reduce}%")
    end

    def report_error(line)
      @shell.say_status(@error_type, line.chomp, @error_type == :error ? :red : :yellow)
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
      @shell.say
      @shell.print_table(table)
      @shell.say
    end
  end
end
