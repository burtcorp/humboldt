# encoding: utf-8

module Humboldt
  class Processor
    class << self
      def self.type_accessor(*names)
        names.each do |name|
          module_eval <<-EOA
            def #{name}
              @#{name} || superclass.#{name}
            end
            def #{name}=(type)
              @#{name} = TypeConverter[type]
              define_method(:#{name}_accessor) do
                TypeConverter[type].new
              end
            end
          EOA
        end
      end

      type_accessor :input_key, :input_value, :output_key, :output_value

      def input(*types)
        self.input_key = types.first
        self.input_value = types.last
      end

      def output(*types)
        self.output_key = types.first
        self.output_value = types.last
      end

      def setup(&block)
        define_method(:instance_setup, &block)
        private(:instance_setup)
      end

      def cleanup(&block)
        define_method(:instance_cleanup, &block)
        private(:instance_cleanup)
      end
    end

    attr_reader :current_context

    def setup(context)
      @current_context = context
      @in_key = input_key_accessor
      @in_value = input_value_accessor
      @out_key = output_key_accessor
      @out_value = output_value_accessor
      create_symlinks!
      instance_setup
    end

    def cleanup(context)
      instance_cleanup
    end

    protected

    def emit(key, value)
      @out_key.ruby = key
      @out_value.ruby = value
      @current_context.write(@out_key.hadoop, @out_value.hadoop)
    end

    private

    def instance_setup
    end

    def instance_cleanup
    end

    def create_symlinks!
      distributed_cache = ::Hadoop::FileCache::DistributedCache
      files = distributed_cache.get_cache_files(@current_context.configuration)
      local_files = distributed_cache.get_local_cache_files(@current_context.configuration)
      if files && local_files
        work_dir = ENV['HADOOP_WORK_DIR']
        files.each_with_index do |file, i|
          target = local_files[i].to_s
          link_path = File.join(work_dir, file.fragment)
          FileUtils.mkdir_p(File.dirname(link_path))
          unless File.exists?(link_path)
            FileUtils.ln_s(target, link_path)
          end
        end
      end
    end
  end
end
