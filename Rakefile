# encoding: utf-8

require 'bundler/setup'


namespace :release do
  PROJECT_NAME = Dir['*.gemspec'].first.split('.').first

  task :tag do
    spec = eval(File.read("#{PROJECT_NAME}.gemspec"))
    version_string = "v#{spec.version.to_s}"
    unless %x(git tag -l).split("\n").include?(version_string)
      system %(git tag -a #{version_string} -m #{version_string})
    end
    system %(git push && git push --tags)
  end

  task :gem do
    mkdir_p 'pkg'
    system %(gem build #{PROJECT_NAME}.gemspec && gem inabox #{PROJECT_NAME}-*.gem && mv #{PROJECT_NAME}-*.gem pkg)
  end
end

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
    Dir.chdir('spec/integration/test_project') do
      command = (<<-END).lines.map(&:strip).join(' && ')
      rvm gemset create humboldt-test_project
      rvm $RUBY_VERSION@humboldt-test_project do gem install bundler
      rvm $RUBY_VERSION@humboldt-test_project do bundle install
      END
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
end

task :release => ['release:tag', 'release:gem']
