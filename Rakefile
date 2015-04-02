# encoding: utf-8

require 'bundler'
require 'bundler/setup'
require 'ant'
require 'rspec/core/rake_task'

namespace :build do
  source_dir = 'ext/src'
  build_dir = 'ext/build'
  ruby_dir = 'lib'

  task :setup do
    mkdir_p build_dir
    ant.path :id => 'compile.class.path' do
      pathelement :location => File.join(ENV['MY_RUBY_HOME'], 'lib', 'jruby.jar')
      File.foreach(File.expand_path('../.classpath', __FILE__)) do |path|
        pathelement :location => path.chop!
      end
    end
  end

  task :compile => :setup do
    ant.javac :destdir => build_dir, :includeantruntime => 'no', :target => '1.6', :source => '1.6', :debug => 'on' do
      classpath :refid => 'compile.class.path'
      src { pathelement :location => source_dir }
    end
  end

  task :jars => :compile do
    ant.jar :destfile => 'lib/humboldt.jar', :basedir => build_dir do
      fileset :dir => build_dir, :includes => '**/*.class'
    end
  end

  task :clean do
    rm_rf build_dir
    rm Dir['lib/humboldt*.jar']
  end
end

desc 'Build native extensions'
task :build => 'build:jars'

namespace :gem do
  Bundler::GemHelper.install_tasks
end

task 'gem:build' => 'build:jars'

desc 'Release a new gem version'
task :release => [:spec, 'gem:release']

namespace :setup do
  hadoop_release = ENV['HADOOP_RELEASE'] || 'hadoop-1.0.3/hadoop-1.0.3-bin'
  current_link = 'tmp/hadoop-current'

  task :hadoop do
    hadoop_url = "http://archive.apache.org/dist/hadoop/common/#{hadoop_release}.tar.gz"
    target_dir = "tmp/#{File.dirname(hadoop_release)}"
    FileUtils.mkdir_p(target_dir)
    Dir.chdir(target_dir) do
      unless File.exists?("#{File.basename(hadoop_release)}.tar.gz")
        command = (<<-END).lines.map(&:strip).join(' && ')
        curl --progress-bar -O '#{hadoop_url}'
        tar xf hadoop*.tar.gz
        END
        system(command)
      end
    end
    FileUtils.rm_f(current_link)
    FileUtils.ln_sf(Dir["#{target_dir}/hadoop*"].sort.first[4..-1], current_link)
  end

  task :test_project do
    Dir.chdir('spec/fixtures/test_project') do
      command = %Q{bash -l -c 'rvm use $RUBY_VERSION && rvm gemset create humboldt-test_project && rvm $RUBY_VERSION@humboldt-test_project do gem install bundler'}
      puts command
      Bundler.clean_system(command)
    end
  end

  task :classpath do
    File.open('.classpath', 'w') do |io|
      %x(#{current_link}/bin/hadoop classpath).chomp.split(':').each do |pattern|
        Dir[pattern].each do |path|
          io.puts(path)
        end
      end
    end
  end
end

desc 'Download Hadoop and set up the classpath configuration'
task :setup => ['setup:hadoop', 'setup:test_project', 'setup:classpath']

RSpec::Core::RakeTask.new(:spec) do |r|
  r.rspec_opts = '--tty'
  r.pattern = 'spec/{integration,humboldt}/**/*_spec.rb'
end

desc 'Build extensions and run the specs'
task :spec => 'gem:build'
