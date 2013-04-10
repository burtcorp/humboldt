# encoding: utf-8

module Humboldt
  class Reducer < Processor
    class << self
      def reduce(&block)
        define_method(:reduce) do |key, values, context|
          @in_key.hadoop = key
          values_enumerator = TypeConversionEnumerator.new(@in_value, values.iterator)
          instance_exec(@in_key.ruby, values_enumerator, &block)
        end
      end
    end

    class TypeConversionEnumerator < Enumerator
      def initialize(*args)
        @value_converter, @hadoop_iterator = args
      end

      def each
        while @hadoop_iterator.has_next
          @value_converter.hadoop = @hadoop_iterator.next
          yield @value_converter.ruby
        end
      end

      def next
        raise StopIteration unless @hadoop_iterator.has_next
        @value_converter.hadoop = @hadoop_iterator.next
        @value_converter.ruby
      end
    end
  end
end
