# encoding: utf-8

module Humboldt
  class Mapper < Processor
    class << self
      def map(&block)
        define_method(:map) do |key, value, context|
          @in_key.hadoop = key
          @in_value.hadoop = value
          instance_exec(@in_key.ruby, @in_value.ruby, &block)
        end
      end
    end
  end
end
