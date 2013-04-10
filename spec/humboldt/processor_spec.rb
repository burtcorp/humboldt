# encoding: utf-8

require 'spec_helper'


module Humboldt
  class TestProcessor < Processor
    self.input_key = :text
  end

  class BaseProcessor < Processor
    attr_reader :trace

    def initialize
      @trace = []
    end

    setup do
      @trace << 'base_setup'
    end

    cleanup do
      @trace << 'base_cleanup'
    end
  end

  class DerivedProcess < BaseProcessor
    input :text, :text
    output :text, :text

    setup do
      super
      @trace << 'derived_setup'
    end

    cleanup do
      @trace << 'derived_cleanup'
      super
    end
  end

  describe Processor do
    context '.type_accessor' do
      it 'defines class method returning accessor class' do
        TestProcessor.input_key.should == TypeConverter::Text
      end

      it 'defines instance method returning accessor instance' do
        TestProcessor.new.input_key_accessor.should be_a(TypeConverter::Text)
      end
    end

    context 'setup and cleanup' do
      it 'supports super for invoking superclass method' do
        processor = DerivedProcess.new
        processor.stub(:create_symlinks!)
        context = stub(:context)
        processor.setup(context)
        processor.cleanup(context)
        processor.trace == %w[base_setup derived_setup derived_cleanup base_cleanup]
      end
    end
  end
end
