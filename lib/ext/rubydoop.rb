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
      STDERR.puts "Warning! Using `format: :combined_text` will not work with remote input paths (e.g. S3) and Hadoop 1.x." if format == :combined_text
      unless format.nil? or format.is_a?(Class)
        class_name = format.to_s.gsub(/^.|_./) {|x| x[-1,1].upcase } + "InputFormat"
        options[:format] = Humboldt::JavaLib.const_get(class_name)
      end
      inputtt(paths, options)
    end

    def enable_compression!
      unless local_mode?
        set 'mapred.compress.map.output', true
        set 'mapred.output.compress', true
        set 'mapred.map.output.compression.codec', 'org.apache.hadoop.io.compress.GzipCodec'
        set 'mapred.output.compression.codec', 'org.apache.hadoop.io.compress.GzipCodec'
        set 'mapred.output.compression.type', 'BLOCK'
      end
    end

    def local_mode?
      @job.configuration.get('mapred.job.tracker') == 'local'
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
  end
end
