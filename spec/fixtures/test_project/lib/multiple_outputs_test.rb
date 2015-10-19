# encoding: utf-8

require 'humboldt'

module MultipleOutputsTest
  USER_ID_LENGTH = 5

  class Mapper < Humboldt::Mapper
    include Humboldt::MultipleOutputs

    input :long, :text
    output :text, :text

    setup do
      setup_multiple_outputs
    end

    cleanup do
      cleanup_multiple_outputs
    end

    map do |_, value|
      multiple_outputs.write('specialoutput0', Hadoop::Io::Text.new('output-key-0'), Hadoop::Io::Text.new('output-value-0'), 'special-path-0')
      emit('key', value)
    end
  end

  class Reducer < Humboldt::Reducer
    include Humboldt::MultipleOutputs

    input :text, :text
    output :text, :text

    setup do
      setup_multiple_outputs
    end

    cleanup do
      cleanup_multiple_outputs
    end

    reduce do |key, values|
      multiple_outputs.write('specialoutput1', Hadoop::Io::Text.new('output-key-1'), Hadoop::Io::Text.new('output-value-1'), 'special-path-1')
      multiple_outputs.write('specialoutput2', Hadoop::Io::Text.new('output-key-2'), Hadoop::Io::Text.new('output-value-2'), 'special-path-2')
      multiple_outputs.write('specialoutput3', Hadoop::Io::LongWritable.new(3), Hadoop::Io::Text.new('output-value-3'), 'special-path-3')
      multiple_outputs.write('specialoutput4', Hadoop::Io::Text.new('output-key-4'), Hadoop::Io::LongWritable.new(4), 'special-path-4')
      multiple_outputs.write('specialoutput5', Hadoop::Io::LongWritable.new(5), Hadoop::Io::LongWritable.new(5), 'special-path-5')
    end
  end
end

Rubydoop.configure do |input_path, output_path|
  job 'test multiple outputs' do
    input input_path
    output output_path, lazy: true

    mapper MultipleOutputsTest::Mapper
    reducer MultipleOutputsTest::Reducer

    named_output 'specialoutput0'
    named_output 'specialoutput1'
    named_output 'specialoutput2', format: :sequence_file
    named_output 'specialoutput3', output_key: :long
    named_output 'specialoutput4', output_value: :long
    named_output 'specialoutput5', output_key: :long, output_value: :long
  end
end
