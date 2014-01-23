# encoding: utf-8

require 'rake'
require 'spec_helper'

describe 'Packaging and running a project' do
  def isolated_run(dir, cmd)
    Dir.chdir(dir) do
      Bundler.clean_system('find $PWD')
      Bundler.clean_system("echo PATH=$GEM_HOME/bin:$PWD/../../../tmp/hadoop-current/bin:$PATH HUMBOLDT_TEST_PROJECT_PATH=$PWD rvm $RUBY_VERSION@humboldt-test_project do env")
      Bundler.clean_system("PATH=$GEM_HOME/bin:$PWD/../../../tmp/hadoop-current/bin:$PATH HUMBOLDT_TEST_PROJECT_PATH=$PWD rvm $RUBY_VERSION@humboldt-test_project do env")
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
  end

  around do |example|
    Dir.chdir(test_project_dir) do
      example.run
    end
  end

  context 'Running the project' do
    before :all do
      isolated_run(test_project_dir, "humboldt run-local --cleanup-before --data-path=data --input='input/*' 2>&1 | tee data/log")
    end

    context 'file caching' do
      it 'successfully reads cached files from mapper' do
        File.readlines('data/test_project/output/part-r-00000').first.should == "mapper\tsecrit/not so secrit\n"
      end

      it 'successfully reads cached files from reducer' do
        File.readlines('data/test_project/output/part-r-00000').last.should == "reducer\tsecrit/not so secrit\n"
      end
    end
  end
end
