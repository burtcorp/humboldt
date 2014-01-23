# encoding: utf-8

require 'humboldt'

module DistributedCacheTest
  class Mapper < Humboldt::Mapper
    input :long, :text
    output :text, :text

    setup do
      @important = File.read('important.doc')
    end

    map do |key, value|
      other = File.read('another_file')
      emit('mapper', @important + '/' + other)
    end
  end

  class Reducer < Humboldt::Reducer
    input :text, :text
    output :text, :text

    setup do
      @important = File.read('important.doc')
    end

    reduce do |key, values|
      emit key, values.next
      other = File.read('another_file')
      emit('reducer', @important + '/' + other)
    end
  end
end

Rubydoop.configure do |input_path, output_path|
  job 'test distributed cache' do
    input input_path
    output output_path

    cache_file File.expand_path("cached_files/super_important_file.doc", ENV['HUMBOLDT_TEST_PROJECT_PATH']), :as => 'important.doc'
    cache_file File.expand_path("cached_files/another_file", ENV['HUMBOLDT_TEST_PROJECT_PATH']), :as => 'another_file'

    mapper DistributedCacheTest::Mapper
    reducer DistributedCacheTest::Reducer
  end
end
