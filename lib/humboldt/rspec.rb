# encoding: utf-8

require 'humboldt'


module RunnerHelpers
  def run_nokey_mapper(mapper, *values, &context_callback)
    key = mapper.input_key_accessor.ruby
    args = values.map { |value| [key, value] }
    run_mapper(mapper, *args, &context_callback)
  end

  def run_mapper(mapper, *entries, &context_callback)
    in_value = mapper.input_value_accessor
    run(mapper, :map, context_callback, *entries) do |value|
      in_value.ruby = value
      in_value.hadoop
    end
  end

  def run_reducer(reducer, *entries, &context_callback)
    run(reducer, :reduce, context_callback, *entries) do |value|
      fake_iterator(*value.map do |v|
        in_value = reducer.input_value_accessor
        in_value.ruby = v
        in_value.hadoop
      end)
    end
  end

  def run(runner, method, context_callback, *entries)
    in_key = runner.input_key_accessor
    context = FakeContext.new(runner.output_key_accessor, runner.output_value_accessor)
    context_callback.call(context) if context_callback
    runner.setup(context)
    entries.each do |entry|
      in_key.ruby = entry.first
      runner.send(method, in_key.hadoop, yield(entry.last), context)
    end
    runner.cleanup(context)
    context.results
  end

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

  class FakeContext
    attr_reader :results, :counters

    def initialize(key_accessor, value_accessor)
      @key_accessor, @value_accessor = key_accessor, value_accessor
      @results = []
      @counters = Hash.new { |h,k| h[k] = Hash.new { |h2,k2| h2[k2] = 0 } }
    end

    def write(key, value)
      @key_accessor.hadoop = key
      @value_accessor.hadoop = value
      @results << [@key_accessor.ruby, @value_accessor.ruby]
    end

    def configuration
      @configuration ||= ::Hadoop::Conf::Configuration.new.tap do |config|
        config.set('mapreduce.framework.name', 'local')
      end
    end

    def get_counter(group, name)
      FakeCounter.new do |amount|
        @counters[group][name] += amount
      end
    end
  end

  class FakeCounter
    def initialize(&block)
      @block = block
    end

    def increment(*args)
      @block.call(*args)
    end
  end
end

RSpec.configure do |conf|
  conf.include(RunnerHelpers)
end
