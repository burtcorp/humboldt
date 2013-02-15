# encoding: utf-8

require 'zlib'


module Humboldt
  class BinaryPrefixPartitioner
    def initialize(cutoff_index)
      @cutoff_index = cutoff_index
    end

    def partition(key, value, num_partitions)
      length = @cutoff_index > key.length ? key.length : @cutoff_index
      prefix = String.from_java_bytes(key.bytes)[0, length]
      Zlib.crc32(prefix) % num_partitions
    end
  end

  class DropBinaryPrefixPartitioner < BinaryPrefixPartitioner
    def partition(key, value, num_partitions)
      length = key.length > @cutoff_index ? key.length - @cutoff_index : 0
      prefix = String.from_java_bytes(key.bytes)[0, length]
      Zlib.crc32(prefix) % num_partitions
    end
  end

  class BinaryPrefixComparator
    def initialize(cutoff_index)
      @cutoff_index = cutoff_index
    end

    def compare_raw(bytes1, start1, length1, bytes2, start2, length2)
      subset_length1 = @cutoff_index > length1 ? length1 : @cutoff_index
      subset_length2 = @cutoff_index > length2 ? length2 : @cutoff_index
      ::Hadoop::Io::WritableComparator.compareBytes(bytes1, start1, subset_length1, bytes2, start2, subset_length2)
    end
  end

  class DropBinaryPrefixComparator < BinaryPrefixComparator
    def compare_raw(bytes1, start1, length1, bytes2, start2, length2)
      subset_length1 = length1 - @cutoff_index
      subset_length2 = length2 - @cutoff_index
      ::Hadoop::Io::WritableComparator.compareBytes(bytes1, start1, subset_length1, bytes2, start2, subset_length2)
    end
  end
end
