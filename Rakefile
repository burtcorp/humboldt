# encoding: utf-8

require 'bundler/setup'
require 'ant'

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

desc 'Build the lib/humboldt.jar'
task :build => 'build:jars'

namespace :gem do
  PROJECT_NAME = Dir['*.gemspec'].first.split('.').first

  task :tag do
    spec = eval(File.read("#{PROJECT_NAME}.gemspec"))
    version_string = "v#{spec.version.to_s}"
    unless %x(git tag -l).split("\n").include?(version_string)
      system %(git tag -a #{version_string} -m #{version_string})
    end
    system %(git push && git push --tags)
  end

  task :build do
    mkdir_p 'pkg'
    system %(gem build #{PROJECT_NAME}.gemspec && mv #{PROJECT_NAME}-*.gem pkg)
  end

  task :inabox => :build do
    system %(gem inabox pkg/#{PROJECT_NAME}-*.gem)
  end

  desc "Tag and release a new gem inabox"
  task :release => [:tag, :inabox]
end
task 'gem:build' => 'build:jars'

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

desc 'Download Hadoop and set up classpath'
task :setup => ['setup:hadoop', 'setup:test_project', 'setup:classpath']

require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |r|
  r.rspec_opts = '--tty'
  r.pattern = 'spec/{integration,humboldt}/**/*_spec.rb'
end

desc "Run the specs"
task :spec => 'gem:build'
