# encoding: utf-8

module Humboldt
  module TypeConverter
    class Binary
      HADOOP = ::Hadoop::Io::BytesWritable
      RUBY = ::String

      attr_reader :hadoop

      def hadoop=(value)
        unless value.is_a?(HADOOP)
          raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{HADOOP}"
        end
        @hadoop = value
      end

      def initialize
        @hadoop = HADOOP.new
      end

      def ruby
        String.from_java_bytes(@hadoop.bytes).byteslice(0, @hadoop.length)
      end

      def ruby=(value)
        unless value.is_a?(RUBY)
          raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{RUBY}"
        end

        @hadoop.set(value.to_java_bytes, 0, value.bytesize)
      end
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
    rescue LoadError
    end

    class Text
      HADOOP = ::Hadoop::Io::Text
      RUBY = ::String

      attr_reader :hadoop

      def hadoop=(value)
        unless value.is_a?(HADOOP)
          raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{HADOOP}"
        end
        @hadoop = value
      end

      def initialize
        @hadoop = HADOOP.new
      end

      def ruby
        String.from_java_bytes(@hadoop.bytes).byteslice(0, @hadoop.length).force_encoding(Encoding::UTF_8)
      end

      def ruby=(value)
        unless value.is_a?(RUBY)
          raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{RUBY}"
        end

        if value.encoding == Encoding::UTF_8
          @hadoop.set(value.to_java_bytes, 0, value.bytesize)
        else
          @hadoop.set(value)
        end
      end
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

      attr_reader :hadoop

      def hadoop=(value)
        unless value.is_a?(HADOOP)
          raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{HADOOP}"
        end
        @hadoop = value
      end

      def initialize
        @hadoop = HADOOP.new
      end

      def ruby
        @hadoop.get
      end

      def ruby=(value)
        unless value.is_a?(Integer)
          raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{RUBY}"
        end

        @hadoop.set value
      end
    end

    class None
      HADOOP = ::Hadoop::Io::NullWritable
      RUBY = ::NilClass

      def hadoop
        HADOOP.get
      end

      def hadoop=(value)
        unless value.is_a?(HADOOP)
          raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{HADOOP}"
        end
      end

      def ruby
        nil
      end

      def ruby=(value)
        unless value.nil?
          raise ArgumentError, "Hadoop type mismatch, was #{value.class}, expected #{RUBY}"
        end
      end
    end

    TYPE_CONVERTER_CLASS_CACHE = Hash.new { |h,k| h[k] = const_get(k.to_s.capitalize) }

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
