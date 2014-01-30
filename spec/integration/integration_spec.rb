# encoding: utf-8

require 'rake'
require 'spec_helper'

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
    FileUtils.rm_rf File.join(test_project_dir, 'build')
    FileUtils.rm_rf File.join(test_project_dir, 'data/test_project/output')
    FileUtils.rm_rf File.join(test_project_dir, 'data/log')
    FileUtils.rm_rf File.join(test_project_dir, 'important.doc')
    FileUtils.rm_rf File.join(test_project_dir, 'another_file')
    FileUtils.rm_rf File.join(test_project_dir, 'Gemfile.lock')

    isolated_run(test_project_dir, "gem install ../../../pkg/*.gem")
    isolated_run(test_project_dir, "bundle install")
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
    end
  end

  context 'Running the project' do
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
        log.should include('Warning! Using `format: :combined_text` will not work with remote input paths (e.g. S3) and Hadoop 1.x.')
      end

      it 'outputs the expected result' do
        File.readlines('data/test_project/output/combined_text/part-r-00000').last.should == "key\t1 2\n"
      end
    end
  end
end
