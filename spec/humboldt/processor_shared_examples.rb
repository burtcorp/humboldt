# encoding: utf-8

require 'spec_helper'


module Humboldt
  shared_examples 'all processors' do |processor_class|
    context 'when using cached files from the distributed cache' do
      let :processor do
        processor_class.new
      end

      around do |example|
        Dir.mktmpdir do |dir|
          ENV['HADOOP_WORK_DIR'] = dir
          Dir.chdir(dir, &example)
          ENV.delete('HADOOP_WORK_DIR')
        end
      end

      before do
        FileUtils.mkdir_p('cached_files/stuff')
        File.write('cached_files/stuff/super_important_file.doc', 'secrit')
        File.write('cached_files/stuff/another_file', 'not so secrit')
      end

      it 'symlinks in the files from the distributed cache before #setup' do
        cache_uris = [stub(fragment: 'important.doc'), stub(fragment: 'another_file')]
        local_uris = %w[cached_files/stuff/super_important_file.doc cached_files/stuff/another_file]
        Hadoop::FileCache::DistributedCache.stub(:get_cache_files).with(configuration).and_return(cache_uris)
        Hadoop::FileCache::DistributedCache.stub(:get_local_cache_files).with(configuration).and_return(local_uris)
        results = []
        context.stub(:write) { |_, v| results << v.to_s }
        processor.setup(context)
        if processor_class.respond_to?(:reduce)
          processor.reduce(::Hadoop::Io::Text.new("hello"), fake_iterator(::Hadoop::Io::Text.new("world")), context)
        else
          processor.map(::Hadoop::Io::Text.new("hello"), ::Hadoop::Io::Text.new("world"), context)
        end
        results.should == ['secrit', 'not so secrit']
      end
    end
  end
end