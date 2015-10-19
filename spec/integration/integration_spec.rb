# encoding: utf-8

require 'rake'
require 'spec_helper'
require 'yaml'

describe 'Packaging and running a project' do
  def isolated_run(dir, cmd)
    Dir.chdir(dir) do
      Bundler.clean_system("PATH=$GEM_HOME/bin:$PWD/../../../tmp/hadoop-current/bin:$PATH HUMBOLDT_TEST_PROJECT_PATH=$PWD rvm $RUBY_VERSION@humboldt-test_project do #{cmd}")
    end
  end

  let :test_project_dir do
    File.expand_path('../../fixtures/test_project', __FILE__)
  end

  before :all do
    FileUtils.rm_rf File.join(test_project_dir, '.humboldt.yml')
    FileUtils.rm_rf File.join(test_project_dir, 'build')
    FileUtils.rm_rf File.join(test_project_dir, 'data/test_project/output')
    FileUtils.rm_rf File.join(test_project_dir, 'data/another_job_config/output')
    FileUtils.rm_rf File.join(test_project_dir, 'data/log')
    FileUtils.rm_rf File.join(test_project_dir, 'important.doc')
    FileUtils.rm_rf File.join(test_project_dir, 'another_file')
    FileUtils.rm_rf File.join(test_project_dir, 'Gemfile.lock')

    isolated_run(test_project_dir, "gem install ../../../pkg/*#{Humboldt::VERSION}*.gem")
    isolated_run(test_project_dir, "bundle install --retry 3")
    isolated_run(test_project_dir, "bundle exec humboldt package")
  end

  around do |example|
    Dir.chdir(test_project_dir) do
      example.run
    end
  end

  context 'Project package' do
    let :jar do
      Java::JavaUtilJar::JarFile.new(Java::JavaIo::File.new(File.expand_path('build/test_project.jar')))
    end

    let :jar_entries do
      jar.entries.to_a.map(&:name)
    end

    it 'includes the built humboldt JAR file' do
      jar_entries.should include('lib/humboldt.jar')
    end

    it 'includes the relevant project files' do
      jar_entries.should include("test_project.rb")
      jar_entries.should include("distributed_cache_test.rb")
      jar_entries.should include("combined_text_test.rb")
      jar_entries.should include("another_job_config.rb")
    end
  end

  context 'Running the project jobs' do
    before :all do
      isolated_run(test_project_dir, "bundle exec humboldt run-local --cleanup-before --skip-package --data-path=data --input='input' 2>&1 | tee data/log")
    end

    let :log do
      File.read('data/log')
    end

    context 'file caching job' do
      it 'successfully reads cached files from mapper' do
        File.readlines('data/test_project/output/cache/part-r-00000').first.should == "mapper\tsecrit/not so secrit\n"
      end

      it 'successfully reads cached files from reducer' do
        File.readlines('data/test_project/output/cache/part-r-00000').last.should == "reducer\tsecrit/not so secrit\n"
      end
    end

    context 'combined text input job' do
      it 'combines both input files into a single map task (Mapper setup block only invoked once)' do
        log.scan(/!!! combined mapper setup/).length.should == 1
      end

      it 'prints a warning about the combined text input only being compatible with Hadoop 2.2.0' do
        log.should match(/Warning! Using `format: :combined_text` will not work with remote input paths \(e.g. S3\) and Hadoop 1.x./)
      end

      it 'outputs the expected result' do
        File.readlines('data/test_project/output/combined_text/part-r-00000').last.should == "key\t1 2\n"
      end
    end
  end

  context 'Running with a project configuration' do
    CONFIG = {
      job_config: 'another_job_config'
    }

    before :all do
      File.open(File.join(test_project_dir, '.humboldt.yml'), 'w') do |f|
        YAML.dump(CONFIG, f)
      end
      isolated_run(test_project_dir, "bundle exec humboldt run-local --cleanup-before --skip-package --data-path=data --input='input/combined_text' 2>&1 | tee data/log")
    end

    after :all do
      FileUtils.rm_rf File.join(test_project_dir, '.humboldt.yml')
    end

    it 'uses the job config directive from the configuration file' do
      expect(File.directory?('data/another_job_config/output')).to be_true
    end
  end

  context 'Using secondary sort' do
    VISITS = [
      ['acme.net', 'AUSER'],
      ['acme.net', 'BUSER'],
      ['acme.net', 'BUSER'],
      ['acme.net', 'CUSER'],
      ['evilcorp.com', 'AUSER'],
      ['evilcorp.com', 'AUSER'],
      ['example.com', 'AUSER'],
      ['example.com', 'BUSER'],
    ].freeze

    before :all do
      input_path = File.join(test_project_dir, 'data/secondary_sort_test/visits.csv')
      FileUtils.mkdir_p(File.dirname(input_path))
      File.open(input_path, 'w') do |io|
        VISITS.shuffle.each do |site, user_id|
          io.puts("#{site},#{user_id}")
        end
      end
      isolated_run(test_project_dir, "bundle exec humboldt run-local --job-config=secondary_sort_test --cleanup-before --skip-package --data-path='' --input='#{input_path}' 2>&1 | tee data/log")
    end

    it 'partitions and groups on part of the map output key' do
      pairs = File.readlines('data/secondary_sort_test/output/part-r-00000').map { |line| line.chomp.split("\t") }
      pairs.should include(%w[acme.net 3])
      pairs.should include(%w[evilcorp.com 1])
      pairs.should include(%w[example.com 2])
    end
  end

  context 'Using multiple outputs' do
    def read_sequence_file(path)
      uri = "file://#{File.expand_path(path)}"
      conf = Hadoop::Conf::Configuration.new
      fs = Hadoop::Fs::FileSystem.get(java.net.URI.create(uri), conf)
      path = Hadoop::Fs::Path.new(uri)
      reader = Hadoop::Io::SequenceFile::Reader.new(fs, path, conf)
      key = Humboldt::TypeConverter.from_hadoop(reader.key_class.ruby_class).new
      value = Humboldt::TypeConverter.from_hadoop(reader.value_class.ruby_class).new
      while reader.next(key.hadoop, value.hadoop)
        yield key.ruby, value.ruby
      end
    end

    before :all do
      input_path = File.join(test_project_dir, 'data/multiple_outputs_test/input')
      FileUtils.mkdir_p(File.dirname(input_path))
      File.open(input_path, 'w') do |io|
        io.puts('foo')
      end
      isolated_run(test_project_dir, "bundle exec humboldt run-local --job-config=multiple_outputs_test --cleanup-before --skip-package --data-path='' --input='#{input_path}' 2>&1 | tee data/log")
    end

    it 'uses the named outputs from both the mapper and reducer' do
      outputs = Dir['data/multiple_outputs_test/output/*-*'].map { |path| File.basename(path) }
      outputs.should include('special-path-0-m-00000')
      outputs.should include('special-path-1-r-00000')
      outputs.should include('special-path-2-r-00000')
      outputs.should include('special-path-3-r-00000')
      outputs.should include('special-path-4-r-00000')
      outputs.should include('special-path-5-r-00000')
    end

    it 'outputs the right kind of types' do
      File.read('data/multiple_outputs_test/output/special-path-0-m-00000').should eq("output-key-0\toutput-value-0\n")
      File.read('data/multiple_outputs_test/output/special-path-1-r-00000').should eq("output-key-1\toutput-value-1\n")
      File.read('data/multiple_outputs_test/output/special-path-3-r-00000').should eq("3\toutput-value-3\n")
      File.read('data/multiple_outputs_test/output/special-path-4-r-00000').should eq("output-key-4\t4\n")
      File.read('data/multiple_outputs_test/output/special-path-5-r-00000').should eq("5\t5\n")
    end

    it 'outputs the right kind of formats' do
      contents = []
      read_sequence_file('data/multiple_outputs_test/output/special-path-2-r-00000') do |key, value|
        contents << [key, value]
      end
      contents.should eq([['output-key-2', 'output-value-2']])
    end
  end
end
