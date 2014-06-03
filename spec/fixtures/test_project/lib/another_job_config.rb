# encoding: utf-8

require 'humboldt'

require 'combined_text_test'

Rubydoop.configure do |input_path, output_path|
  job 'test job' do
    input input_path, format: :text
    output output_path

    mapper CombinedTextTest::Mapper
    reducer CombinedTextTest::Reducer
  end
end
