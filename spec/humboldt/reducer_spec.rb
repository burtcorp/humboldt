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
