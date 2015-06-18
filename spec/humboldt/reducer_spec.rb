# encoding: utf-8

require 'spec_helper'

module Humboldt
  class EchoReducer < Reducer
    input :long, :text
    output :long, :text

    reduce do |key, values|
      values.each do |value|
        emit(key, value)
      end
    end
  end

  class PairwiseEchoReducer < Reducer
    input :long, :text
    output :long, :text

    reduce do |key, values|
      loop do
        v1, v2 = values.next, values.next
        emit(key, v1 + v2)
      end
    end
  end

  class ReversingTestReducer < Reducer
    input :long, :text
    output :long, :text

    reduce do |key, values|
      emit(key, values.to_a.join(","))
    end
  end

  class WrongOutputTypeReducer < Reducer
    input :text, :text
    output :text, :text
    reduce do |key, values|
      emit(20, 20)
    end
  end

  class SetupCleanupReducer < Reducer
    input :text, :text
    output :text, :text
    reduce do |key, values|
      emit('reduce', 'reduce')
    end

    attr_reader :state
    setup do
      emit('setup', 'setup')
    end
    cleanup do
      emit('cleanup', 'cleanup')
    end
  end

  class DistributedCacheUsingReducer < Reducer
    input :text, :text
    output :text, :text

    setup do
      @important = File.read('important.doc')
    end

    reduce do |key, values|
      other = File.read('another_file')
      emit(key, @important)
      emit(key, other)
    end
  end

  class PackableValuesReducer < Reducer
    FooKey = Key.new(:value)
    FooValue = Value.new(:value)

    input FooKey, FooValue
    output FooKey, FooValue

    attr_writer :on_reduce_value

    reduce do |key, values|
      values.each do |value|
        @on_reduce_value.call(key, value) if @on_reduce_value
        emit key, value
      end
    end
  end

  describe Reducer do
    let :context do
      stub(:context)
    end

    let :configuration do
      stub(:configuration)
    end

    before do
      context.stub(:configuration).and_return(configuration)
      Hadoop::FileCache::DistributedCache.stub(:get_cache_files).and_return(nil)
      Hadoop::FileCache::DistributedCache.stub(:get_local_cache_files).and_return(nil)
    end

    context '#reduce' do
      let :reducer do
        reducer = ReversingTestReducer.new
        reducer.setup(context)
        reducer
      end

      it 'calls context.write' do
        context.should_receive(:write)
        reducer.reduce(::Hadoop::Io::LongWritable.new(42), fake_iterator(::Hadoop::Io::Text.new('Hello'), ::Hadoop::Io::Text.new('World')), context)
      end

      context 'for iterating over the values' do
        let :echo_reducer do
          EchoReducer.new.tap { |r| r.setup(context) }
        end

        let :pairwise_echo_reducer do
          PairwiseEchoReducer.new.tap { |r| r.setup(context) }
        end

        before do
          @keys, @values = [], []
          context.stub(:write) do |key, value|
            @keys << key.get
            @values << value.to_s
          end
        end

        it 'gives the reducer an Enumerable of values' do
          key = ::Hadoop::Io::LongWritable.new(42)
          iterator = fake_iterator(::Hadoop::Io::Text.new('Hello'), ::Hadoop::Io::Text.new('World'))
          echo_reducer.reduce(key, iterator, context)
          @keys.should == [42, 42]
          @values.should == ['Hello', 'World']
        end

        it 'gives the reducer an Enumerator of values' do
          key = ::Hadoop::Io::LongWritable.new(42)
          iterator = fake_iterator(::Hadoop::Io::Text.new('Hello'), ::Hadoop::Io::Text.new('World'), ::Hadoop::Io::Text.new('Foo'), ::Hadoop::Io::Text.new('Bar'))
          pairwise_echo_reducer.reduce(key, iterator, context)
          @keys.should == [42, 42]
          @values.should == ['HelloWorld', 'FooBar']
        end
      end

      it 'converts from Hadoop to Ruby types and back again' do
        context.should_receive(:write) do |key, value|
          key.should be_a(::Hadoop::Io::LongWritable)
          key.get.should == 42
          value.should be_a(::Hadoop::Io::Text)
          value.to_s.should == 'Hello,World'
        end
        reducer.reduce(::Hadoop::Io::LongWritable.new(42), fake_iterator(::Hadoop::Io::Text.new('Hello'), ::Hadoop::Io::Text.new('World')), context)
      end

      it 'borks on input type mismatch' do
        context.stub(:write)
        expect { reducer.reduce(::Hadoop::Io::Text.new('42'), fake_iterator(::Hadoop::Io::Text.new('Hello World')), context) }.to raise_error(/Hadoop type mismatch/)
        expect { reducer.reduce(::Hadoop::Io::LongWritable.new(42), fake_iterator(::Hadoop::Io::LongWritable.new(42)), context) }.to raise_error(/Hadoop type mismatch/)
      end

      it 'borks on output type mismatch' do
        context.stub(:write)
        bad_reducer = WrongOutputTypeReducer.new
        bad_reducer.setup(context)
        expect { bad_reducer.reduce(::Hadoop::Io::Text.new('42'), fake_iterator(::Hadoop::Io::Text.new('Hello World')), context) }.to raise_error(/Hadoop type mismatch/)
      end

      context 'with packable types as input and output' do
        let :reducer do
          reducer = PackableValuesReducer.new
          reducer.setup(context)
          reducer
        end

        let :input_key do
          PackableValuesReducer::FooKey.new('greetings')
        end

        let :input_value1 do
          PackableValuesReducer::FooValue.new('Hello')
        end

        let :input_value2 do
          PackableValuesReducer::FooValue.new('World')
        end

        let :hadoop_key do
          type_converter = TypeConverter[PackableValuesReducer::FooKey].new
          type_converter.ruby = input_key
          type_converter.hadoop
        end

        let :hadoop_value1 do
          type_converter = TypeConverter[PackableValuesReducer::FooValue].new
          type_converter.ruby = input_value1
          type_converter.hadoop
        end

        let :hadoop_value2 do
          type_converter = TypeConverter[PackableValuesReducer::FooValue].new
          type_converter.ruby = input_value2
          type_converter.hadoop
        end

        before do
          context.stub(:write)
        end

        it 'converts input from Hadoop types to the given key and value types' do
          reducer_key = nil
          values = []
          reducer.on_reduce_value = proc do |key, value|
            reducer_key = key
            values << value
          end
          reducer.reduce(hadoop_key, fake_iterator(hadoop_value1, hadoop_value2), context)
          reducer_key.should == input_key
          values.should == [input_value1, input_value2]
        end

        it 'converts given output key and value types to Hadoop types' do
          reducer_key = nil
          values = []
          context.stub(:write) do |key, value|
            reducer_key = key
            values << value.to_s
          end
          reducer.reduce(hadoop_key, fake_iterator(hadoop_value1, hadoop_value2), context)
          reducer_key.should == hadoop_key
          values.should == [hadoop_value1.to_s, hadoop_value2.to_s]
        end
      end
    end

    context 'setup and cleanup' do
      let :reducer do
        reducer = SetupCleanupReducer.new
      end

      it 'delegates to the corresponding blocks passed to the DSL methods' do
        results = []
        context.stub(:write) do |key, value|
          results << key.to_s
        end
        reducer.setup(context)
        reducer.reduce(::Hadoop::Io::Text.new(''), fake_iterator(::Hadoop::Io::Text.new('')), context)
        reducer.cleanup(context)
        results.should == %w[setup reduce cleanup]
      end
    end
  end
end
