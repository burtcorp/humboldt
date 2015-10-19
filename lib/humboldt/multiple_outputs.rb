# encoding: utf-8

module Humboldt
  module MultipleOutputs
    def setup_multiple_outputs
      @multiple_outputs = Hadoop::Mapreduce::Lib::Output::MultipleOutputs.new(current_context)
    end

    def cleanup_multiple_outputs
      @multiple_outputs.close
    end

    def multiple_outputs
      @multiple_outputs
    end
  end
end
