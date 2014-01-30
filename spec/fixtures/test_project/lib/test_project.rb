# encoding: utf-8

require 'humboldt'

require 'distributed_cache_test'

Rubydoop.configure do |input_path, output_path|
  job 'test distributed cache' do
    input "#{input_path}/cache"
    output "#{output_path}/cache"

    cache_file File.expand_path("cached_files/super_important_file.doc", ENV['HUMBOLDT_TEST_PROJECT_PATH']), :as => 'important.doc'
    cache_file File.expand_path("cached_files/another_file", ENV['HUMBOLDT_TEST_PROJECT_PATH']), :as => 'another_file'

    mapper DistributedCacheTest::Mapper
    reducer DistributedCacheTest::Reducer
  end
end
