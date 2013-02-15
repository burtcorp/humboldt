# encoding: utf-8

require 'java'

IO.foreach(File.expand_path('../../.classpath', __FILE__)) { |path| $CLASSPATH << path.chomp }

require 'bundler'; Bundler.setup(:default, :test)

require 'tmpdir'

require 'humboldt'
require 'humboldt/emr_flow'
require 'humboldt/hadoop_status_filter'

module SpecHelpers
  def fake_iterator(*values)
    FakeIterable.new(values)
  end

  class FakeIterable
    def initialize(values)
      @values = values
    end
    def iterator
      FakeIterator.new(@values.dup)
    end
  end

  class FakeIterator
    def initialize(values)
      @values = values
    end
    def has_next
      !@values.empty?
    end
    def next
      @values.shift
    end
  end
end

RSpec.configure do |conf|
  conf.include(SpecHelpers)
end
