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

task :release => ['release:tag', 'release:gem']