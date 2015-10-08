# encoding: utf-8

module Rubydoop
  class JobDefinition
    alias mapperrr mapper
    def mapper(cls)
      map_output_key cls.output_key.const_get(:HADOOP) if cls.respond_to?(:output_key)
      map_output_value cls.output_value.const_get(:HADOOP) if cls.respond_to?(:output_value)
      mapperrr cls
    end

    alias reducerrr reducer
    def reducer(cls)
      output_key cls.output_key.const_get(:HADOOP) if cls.respond_to?(:output_key)
      output_value cls.output_value.const_get(:HADOOP) if cls.respond_to?(:output_value)
      reducerrr cls
    end

    alias inputtt input
    def input(paths, options={})
      options = options.dup
      format = options[:format]
      STDERR.puts "Warning! Using `format: :combined_text` will not work with remote input paths (e.g. S3) and Hadoop 1.x. Cf. https://issues.apache.org/jira/browse/MAPREDUCE-1806" if format == :combined_text
      unless format.nil? or format.is_a?(Class)
        class_name = format.to_s.gsub(/^.|_./) {|x| x[-1,1].upcase } + "InputFormat"
        begin
          options[:format] = Humboldt::JavaLib.const_get(class_name)
        rescue NameError
        end
      end
      inputtt(paths, options)
    end

    def enable_compression!
      unless local_mode?
        if framework == :mapreduce
          set 'mapreduce.map.output.compress', true
          set 'mapreduce.output.fileoutputformat.compress', true
          set 'mapreduce.map.output.compress.codec', 'org.apache.hadoop.io.compress.GzipCodec'
          set 'mapreduce.output.fileoutputformat.compress.codec', 'org.apache.hadoop.io.compress.GzipCodec'
          set 'mapreduce.output.fileoutputformat.compress.type', 'BLOCK'
        else
          set 'mapred.compress.map.output', true
          set 'mapred.output.compress', true
          set 'mapred.map.output.compression.codec', 'org.apache.hadoop.io.compress.GzipCodec'
          set 'mapred.output.compression.codec', 'org.apache.hadoop.io.compress.GzipCodec'
          set 'mapred.output.compression.type', 'BLOCK'
        end
      end
    end

    def framework
      @framework ||= @job.configuration.get('mapreduce.framework.name') ? :mapreduce : :mapred
    end

    def local_mode?
      property = framework == :mapreduce ? 'mapreduce.framework.name' : 'mapred.job.tracker'
      @job.configuration.get(property) == 'local'
    end

    def cache_file(file, options = {})
      symlink = options.fetch(:as, File.basename(file))
      if local_mode? && !Hadoop::Mapreduce::Job.instance_methods.include?(:add_cache_file)
        unless File.symlink?(symlink) && File.readlink(symlink) == file
          FileUtils.ln_s file, symlink
        end
      else
        uri = java.net.URI.new("#{file}\##{symlink}")
        Hadoop::FileCache::DistributedCache.add_cache_file(uri, @job.configuration)
      end
    end

    # Configures the job for secondary sort on the specified slice of the mapper
    # output key.
    #
    # Hadoop comes with a partitioner that can partition the map output based
    # on a slice of the map output key. Humboldt ships with a comparator that
    # uses the same configuration. Together they can be used to implement
    # secondary sort.
    #
    # Secondary sort is a mapreduce pattern where you emit a key, but partition
    # and group only on a subset of that key. This has the result that each
    # reduce invocation will see values grouped by the subset, but ordered by
    # the whole key. It is used, among other things, to efficiently count
    # distinct values.
    #
    # Say you want to count the number of distinct visitors to a site. Your
    # input is pairs of site and visitor IDs. The naïve implementation is to
    # emit the site as key and the visitor ID as value and then, in the reducer,
    # collect all IDs in a set, and emit the site and the size of the set of IDs.
    # This is very memory inefficient, and impractical. For any interesting
    # amount of data you will not be able to keep all the visitor IDs in memory.
    #
    # What you do, instead, is to concatenate the site and visitor ID and emit
    # that as key, and the visitor ID as value. It might seem wasteful to emit
    # the visitor ID twice, but it's necessary since Hadoop will only give you
    # the key for the first value in each group.
    #
    # You then instruct Hadoop to partition and group on just the site part of
    # the key. Hadoop will still sort the values by their full key, so within
    # each group the values will be sorted by visitor ID. In the reducer it's
    # now trivial to loop over the values and just increment a counter each time
    # the visitor ID changes.
    #
    # You configure which part of the key to partition and group by specifying
    # the start and end _indexes_. The reason why they are indexes and not a
    # start index and a length, like Ruby's `String#slice`, is that you also can
    # use negative indexes to count from the end. Negative indexes are useful
    # for example when you don't know how wide the part of the key that you want
    # use is. In the example above if you use the domain to identify sites these
    # can be of different length. If your visitor IDs are 20 characters you can
    # use 0 and -21 as your indexes.
    #
    # @param [Fixnum] start_index The first index of the slice, negative numbers
    #   are counted from the end
    # @param [Fixnum] end_index The last index of the slice, negative numbers
    #   are counted from the end
    # @see http://hadoop.apache.org/docs/r2.7.1/api/org/apache/hadoop/mapreduce/lib/partition/BinaryPartitioner.html Hadoop's BinaryPartitioner
    def secondary_sort(start_index, end_index)
      @job.set_partitioner_class(Hadoop::Mapreduce::Lib::Partition::BinaryPartitioner)
      Hadoop::Mapreduce::Lib::Partition::BinaryPartitioner.set_offsets(@job.configuration, start_index, end_index)
      @job.set_grouping_comparator_class(Humboldt::JavaLib::BinaryComparator)
      Humboldt::JavaLib::BinaryComparator.set_offsets(@job.configuration, start_index, end_index)
    end
  end
end
