# encoding: utf-8

require 'spec_helper'

module Humboldt
  class ReversingTestMapper < Mapper
    input :long, :text
    output :long, :text

    map do |key, value|
      emit(key, value.reverse)
    end
  end

  class WrongOutputTypeMapper < Mapper
    input :text, :text
    output :text, :text
    map do |key, value|
      emit(20, 20)
    end
  end

  class SetupCleanupMapper < Mapper
    input :text, :text
    output :text, :text
    map do |key, value|
      emit('map', 'map')
    end

    attr_reader :state
    setup do
      emit('setup', 'setup')
    end
    cleanup do
      emit('cleanup', 'cleanup')
    end
  end

  class DistributedCacheUsingMapper < Mapper
    input :text, :text
    output :text, :text

    setup do
      @important = File.read('important.doc')
    end

    map do |key, value|
      other = File.read('another_file')
      emit(key, @important)
      emit(key, other)
    end
  end

  describe Mapper do
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

    context '#map' do
      let :mapper do
        mapper = ReversingTestMapper.new
        mapper.setup(context)
        mapper
      end

      it 'calls context.write' do
        context.should_receive(:write)
        mapper.map(::Hadoop::Io::LongWritable.new(42), ::Hadoop::Io::Text.new('Hello World'), context)
      end

      it 'converts from Hadoop to Ruby types and back again' do
        context.should_receive(:write) do |key, value|
          key.should be_a(::Hadoop::Io::LongWritable)
          key.get.should == 42
          value.should be_a(::Hadoop::Io::Text)
          value.to_s.should == 'dlroW olleH'
        end
        mapper.map(::Hadoop::Io::LongWritable.new(42), ::Hadoop::Io::Text.new('Hello World'), context)
      end

      it 'borks on input type mismatch' do
        context.stub(:write)
        expect { mapper.map(::Hadoop::Io::Text.new('42'), ::Hadoop::Io::Text.new('Hello World'), context) }.to raise_error(/Hadoop type mismatch/)
      end

      it 'borks on output type mismatch' do
        context.stub(:write)
        bad_mapper = WrongOutputTypeMapper.new
        bad_mapper.setup(context)
        expect { bad_mapper.map(::Hadoop::Io::Text.new('42'), ::Hadoop::Io::Text.new('Hello World'), context) }.to raise_error(/Hadoop type mismatch/)
      end
    end

    context 'setup and cleanup' do
      let :mapper do
        mapper = SetupCleanupMapper.new
      end

      it 'delegates to the corresponding blocks passed to the DSL methods' do
        results = []
        context.stub(:write) do |key, value|
          results << key.to_s
        end
        mapper.setup(context)
        mapper.map(::Hadoop::Io::Text.new(''), ::Hadoop::Io::Text.new(''), context)
        mapper.cleanup(context)
        results.should == %w[setup map cleanup]
      end
    end
  end
end
