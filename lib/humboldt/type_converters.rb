# encoding: utf-8

module Humboldt
  module TypeConverter
    class Binary
      HADOOP = ::Hadoop::Io::BytesWritable
      RUBY = ::String
    end

    begin
      require 'msgpack'

      class Encoded < Binary
        def ruby=(value)
          unless value.is_a?(Hash) || value.is_a?(Array)
            raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected Hash or Array"
          end
          super(MessagePack.pack(value))
        end

        def ruby
          MessagePack.unpack(super(), encoding: Encoding::UTF_8)
        end
      end

      def self.create_encoded_packable(type)
        cls = Class.new(Encoded) do
          def ruby=(value)
            unless value.is_a?(self.class.type)
              raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{self.class.type}"
            end
            super(value.to_packable)
          end

          def ruby
            self.class.type.from_packable(super())
          end
        end
        cls.define_singleton_method(:type) do
          type
        end
        cls
      end
    rescue LoadError
    end

    class Text
      HADOOP = ::Hadoop::Io::Text
      RUBY = ::String
    end

    begin
      require 'json'

      class Json < Text
        def ruby=(value)
          unless value.is_a?(Hash) || value.is_a?(Array)
            raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected Hash or Array"
          end
          super(JSON.generate(value))
        end

        def ruby
          JSON.parse(super())
        end
      end
    end

    class Long
      HADOOP = ::Hadoop::Io::LongWritable
      RUBY = ::Integer
    end

    class None
      HADOOP = ::Hadoop::Io::NullWritable
      RUBY = ::NilClass
    end

    TYPE_CONVERTER_CLASS_CACHE = Hash.new do |h,k|
      h[k] = begin
        if k.respond_to?(:include?) && k.include?(Humboldt::Packable)
          create_encoded_packable(k)
        else
          const_get(k.to_s.capitalize)
        end
      end
    end

    def self.[](name)
      TYPE_CONVERTER_CLASS_CACHE[name]
    end

    FROM_HADOOP_MAPPINGS = {
      ::Hadoop::Io::Text => Text,
      ::Hadoop::Io::BytesWritable => Binary,
      ::Hadoop::Io::LongWritable => Long,
      ::Hadoop::Io::NullWritable => None
    }.freeze

    def self.from_hadoop(hadoop_class)
      accessor = FROM_HADOOP_MAPPINGS[hadoop_class]
      raise ArgumentError, "Unsupported Hadoop type: #{hadoop_class}" unless accessor
      accessor
    end
  end
end
