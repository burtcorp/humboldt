# encoding: utf-8

require 'spec_helper'


module Humboldt
  describe DropBinaryPrefixPartitioner do
    let :partitioner do
      described_class.new(12)
    end

    describe '#partition' do
      it 'returns a partition based on the part before the user ID suffix' do
        prefix = 'edb72a68a9cfaab392fe8cf95baeb80d,computer'
        key1 = ::Hadoop::Io::Text.new("#{prefix}/LZF4B7Y2F4L1")
        key2 = ::Hadoop::Io::Text.new("#{prefix}/LY2F4L1ZF4B7")
        key3 = ::Hadoop::Io::Text.new("#{prefix}xyz/LY2F4L1ZF4B7")
        partition1 = partitioner.partition(key1, nil, 24)
        partition2 = partitioner.partition(key2, nil, 24)
        partition3 = partitioner.partition(key3, nil, 24)
        partition4 = partitioner.partition(key1, nil, 17)
        partition5 = partitioner.partition(key2, nil, 17)
        partition6 = partitioner.partition(key3, nil, 17)
        partition1.should == partition2
        partition4.should == partition5
        partition3.should_not == partition1
        partition6.should_not == partition4
      end

      it 'handles too short keys' do
        key1 = ::Hadoop::Io::Text.new('MDS0VFYR')
        partition1 = partitioner.partition(key1, nil, 10)
        partition1.should < 10
        partition1.should >= 0
      end
    end
  end

  describe DropBinaryPrefixComparator do
    let :comparator do
      described_class.new(12)
    end

    describe '#compare_raw' do
      it 'returns the sort order by looking only at the part before the user ID suffix' do
        prefix = 'edb72a68a9cfaab392fe8cf95baeb80d,computer'
        key1 = ::Hadoop::Io::Text.new("#{prefix}/LZF4B7Y2F4L1")
        key2 = ::Hadoop::Io::Text.new("#{prefix}/LY2F4L1ZF4B7")
        key3 = ::Hadoop::Io::Text.new("#{prefix}xyz/LY2F4L1ZF4B7")
        order12 = comparator.compare_raw(key1.bytes, 0, key1.length, key2.bytes, 0, key2.length)
        order13 = comparator.compare_raw(key1.bytes, 0, key1.length, key3.bytes, 0, key3.length)
        order12.should == 0
        order13.should be < 0
      end

      it 'handles too short keys' do
        key1 = ::Hadoop::Io::Text.new('MDS0VFYR3')
        key2 = ::Hadoop::Io::Text.new('MDS0VFYR')
        expect { comparator.compare_raw(key1.bytes, 0, key1.length, key1.bytes, 0, key1.length) }.to_not raise_error
        expect { comparator.compare_raw(key1.bytes, 0, key1.length, key2.bytes, 0, key2.length) }.to_not raise_error
      end
    end
  end

  describe BinaryPrefixPartitioner do
    let :partitioner do
      described_class.new(12)
    end

    describe '#partition' do
      it 'returns a partition based on the 12 char ID prefix' do
        key1 = ::Hadoop::Io::Text.new('MDS0VFYR3856000004')
        key2 = ::Hadoop::Io::Text.new('MDS0VFYR3856000005')
        key3 = ::Hadoop::Io::Text.new('MVFY3946RDS0000001')
        partition1 = partitioner.partition(key1, nil, 24)
        partition2 = partitioner.partition(key2, nil, 24)
        partition3 = partitioner.partition(key3, nil, 24)
        partition4 = partitioner.partition(key1, nil, 17)
        partition5 = partitioner.partition(key2, nil, 17)
        partition6 = partitioner.partition(key3, nil, 17)
        partition1.should == partition2
        partition4.should == partition5
        partition3.should_not == partition1
        partition6.should_not == partition4
      end

      it 'handles too short keys' do
        key1 = ::Hadoop::Io::Text.new('MDS0VFYR')
        partition1 = partitioner.partition(key1, nil, 10)
        partition1.should < 10
        partition1.should >= 0
      end
    end
  end

  describe BinaryPrefixComparator do
    let :comparator do
      described_class.new(12)
    end

    describe '#compare_raw' do
      it 'returns the sort order by looking only at first 12 chars' do
        key1 = ::Hadoop::Io::Text.new('MDS0VFYR3856000004')
        key2 = ::Hadoop::Io::Text.new('MDS0VFYR3856000005')
        key3 = ::Hadoop::Io::Text.new('MVFY3956RDS0000001')
        order12 = comparator.compare_raw(key1.bytes, 0, key1.length, key2.bytes, 0, key2.length)
        order13 = comparator.compare_raw(key1.bytes, 0, key1.length, key3.bytes, 0, key3.length)
        order12.should == 0
        order13.should be < 0
      end

      it 'handles too short keys' do
        key1 = ::Hadoop::Io::Text.new('MDS0VFYR3')
        key2 = ::Hadoop::Io::Text.new('MDS0VFYR')
        expect { comparator.compare_raw(key1.bytes, 0, key1.length, key1.bytes, 0, key1.length) }.to_not raise_error
        expect { comparator.compare_raw(key1.bytes, 0, key1.length, key2.bytes, 0, key2.length) }.to_not raise_error
      end
    end
  end
end
