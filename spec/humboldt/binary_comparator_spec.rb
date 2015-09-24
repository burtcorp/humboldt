# encoding: utf-8

require 'spec_helper'

module Humboldt
  module JavaLib
    describe BinaryComparator do
      let :comparator do
        described_class.new
      end

      let :conf do
        Hadoop::Conf::Configuration.new
      end

      let :keys do
        [
          Hadoop::Io::BytesWritable.new("01234".to_java_bytes),
          Hadoop::Io::BytesWritable.new("01235".to_java_bytes),
          Hadoop::Io::BytesWritable.new("01244".to_java_bytes),
          Hadoop::Io::BytesWritable.new("11235".to_java_bytes),
        ]
      end

      let :left_offset do
        0
      end

      let :right_offset do
        -1
      end

      def prefix_size(w)
        case w
        when Hadoop::Io::BytesWritable
          4
        when Hadoop::Io::Text
          Hadoop::Io::WritableUtils.decodeVIntSize(writable_to_bytes(w)[0])
        else
          raise 'Not supported'
        end
      end

      def writable_to_bytes(w)
        Hadoop::Io::WritableUtils.to_byte_array(w)
      end

      def should_compare(k1, k2, assertion)
        compare_writables(k1, k2).should(assertion)
        compare_bytes(k1, k2).should(assertion)
      end

      def compare_writables(k1, k2)
        comparator.compare(k1, k2)
      end

      def compare_bytes(k1, k2)
        b1 = writable_to_bytes(k1)
        b2 = writable_to_bytes(k2)
        comparator.compare(b1, 0, k1.length + prefix_size(k1), b2, 0, k2.length + prefix_size(k1))
      end

      before do
        described_class.set_offsets(conf, left_offset, right_offset)
        conf.set(Hadoop::Mapreduce::MRJobConfig::MAP_OUTPUT_KEY_CLASS, keys.first.java_class.name)
        comparator.set_conf(conf)
      end

      context 'when limiting the comparison to a prefix' do
        let :left_offset do
          0
        end

        let :right_offset do
          3
        end

        it 'returns 0 when comparing a writable to itself' do
          should_compare(keys[0], keys[0], eq(0))
        end

        it 'returns -1 when the first argument is lesser than the second' do
          should_compare(keys[0], keys[3], eq(-1))
        end

        it 'returns 1 when the first argument is greater than the second' do
          should_compare(keys[3], keys[0], eq(1))
        end

        it 'returns 0 when the specified slice of the arguments are equal' do
          should_compare(keys[0], keys[1], eq(0))
        end

        it 'returns -1 when the specified slice of the first argument is lesser than the same slice of second' do
          should_compare(keys[0], keys[2], eq(-1))
        end

        it 'returns 1 when the specified slice of the first argument is greater than the same slice of the second' do
          should_compare(keys[2], keys[0], eq(1))
        end
      end

      context 'when comparing a slice in the middle' do
        let :left_offset do
          1
        end

        let :right_offset do
          3
        end

        it 'returns 0 when comparing a writable to itself' do
          should_compare(keys[0], keys[0], eq(0))
        end

        it 'returns -1 when the first argument is lesser than the second' do
          should_compare(keys[0], keys[2], eq(-1))
        end

        it 'returns 1 when the first argument is greater than the second' do
          should_compare(keys[2], keys[0], eq(1))
        end

        it 'returns 0 when the specified slice of the arguments are equal' do
          should_compare(keys[0], keys[3], eq(0))
        end

        it 'returns -1 when the specified slice of the first argument is lesser than the same slice of second' do
          should_compare(keys[0], keys[2], eq(-1))
        end

        it 'returns 1 when the specified slice of the first argument is greater than the same slice of the second' do
          should_compare(keys[2], keys[0], eq(1))
        end
      end

      context 'with negative offsets' do
        let :keys do
          [
            Hadoop::Io::BytesWritable.new("x4xxx".to_java_bytes),
            Hadoop::Io::BytesWritable.new("xx2xxx".to_java_bytes),
            Hadoop::Io::BytesWritable.new("xxx3xxx".to_java_bytes),
            Hadoop::Io::BytesWritable.new("xxxx1xxx".to_java_bytes),
          ]
        end

        let :left_offset do
          -4
        end

        let :right_offset do
          -3
        end

        it 'returns 0 when comparing a writable to itself' do
          should_compare(keys[0], keys[0], eq(0))
        end

        it 'counts the offsets from the end of the byte arrays' do
          sorted_keys = keys.sort { |k1, k2| compare_writables(k1, k2) }.map { |k| String.from_java_bytes(k.bytes) }
          sorted_keys.should eq(%w[
            xxxx1xxx
            xx2xxx
            xxx3xxx
            x4xxx
          ])
          sorted_keys = keys.sort { |k1, k2| compare_bytes(k1, k2) }.map { |k| String.from_java_bytes(k.bytes) }
          sorted_keys.should eq(%w[
            xxxx1xxx
            xx2xxx
            xxx3xxx
            x4xxx
          ])
        end
      end

      context 'when the writable does not align with the start of the byte array' do
        it 'correctly accounts for the offset' do
          b1 = writable_to_bytes(Hadoop::Io::BytesWritable.new('foobar'.to_java_bytes))
          bytes = String.from_java_bytes(b1)
          bytes.prepend("\xff")
          b2 = bytes.to_java_bytes
          comparator.compare(b1, 0, 10, b2, 1, 10).should eq(0)
        end
      end

      context 'when comparing Text' do
        let :strings do
          [
            ('x' * 3) << "3xxx",
            ('x' * 400) << "2xxx",
            ('x' * 63) << "4xxx",
            ('x' * 14) << "1xxx",
          ]
        end

        let :keys do
          strings.map do |s|
            Hadoop::Io::Text.new(s.to_java_bytes)
          end
        end

        let :left_offset do
          -4
        end

        let :right_offset do
          -3
        end

        it 'returns 0 when comparing a Text to itself' do
          should_compare(keys[0], keys[0], eq(0))
        end

        it 'counts the offsets from the end of the byte arrays' do
          sorted_keys = keys.sort { |k1, k2| compare_writables(k1, k2) }.map { |k| String.from_java_bytes(k.bytes) }
          sorted_keys.should eq(strings.values_at(3, 1, 0, 2))
          sorted_keys = keys.sort { |k1, k2| compare_bytes(k1, k2) }.map { |k| String.from_java_bytes(k.bytes) }
          sorted_keys.should eq(strings.values_at(3, 1, 0, 2))
        end
      end

      context 'when attempting to compare something that is not BinaryComparable' do
        let :left_offset do
          0
        end

        let :right_offset do
          4
        end

        it 'raises an error' do
          i1 = Hadoop::Io::IntWritable.new(3)
          i2 = Hadoop::Io::IntWritable.new(4)
          expect { compare_writables(i1, i2) }.to raise_error(java.lang.ClassCastException)
        end
      end
    end
  end
end

