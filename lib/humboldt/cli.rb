# encoding: utf-8

require 'thor'
require 'aws'
require 'rubydoop/package' # this prints an annoying warning in JRuby 1.7.0.RC1
require 'humboldt/emr_flow'



module Humboldt
  class Cli < Thor
    include Thor::Actions

    desc 'package', 'Package job JAR file'
    def package
      say("Packaging job into #{relative_path(job_package.jar_path)}")
      job_package.create!
    end

    desc 'launch a job', 'run the named job'
    method_option :input, :type => :string, :required => true
    method_option :mode, :default => 'local'
    method_option :output, :type => :string, :desc => 'the output directory, defaults to data/<job config>/output in local mode and s3://<job bucket>/<project name>/output in emr mode'
    method_option :job_config, :type => 'string', :desc => 'the Ruby file containing the job configuration, defaults to the project name'
    method_option :cleanup_before, :type => :boolean, :default => false, :desc => 'automatically remove the output dir before launching'
    method_option :local_completes, :type => :string, :default => 'data/completes'
    def launch
      check_job!
      invoke(:package, [], {})
      case options[:mode].downcase.to_sym
      when :local then launch_local!
      when :emr then launch_emr!
      else raise Thor::Error, "#{mode} not supported"
      end
    end

    private

    def project_jar
      @project_jar ||= Dir['build/*.jar'].reject { |path| path.start_with?('jruby-complete') }.first
    end

    def job_package
      @job_package ||= Rubydoop::Package.new
    end

    def job_config
      options[:job_config] || job_package.project_name
    end

    def s3
      @s3 ||= AWS::S3.new
    end

    def emr
      @emr ||= AWS::EMR.new
    end

    def job_bucket
      @job_bucket ||= s3.buckets['humboldt-emr']
    end

    def data_bucket
      @data_bucket ||= s3.buckets['test-completes-logs']
    end

    def check_job!
      raise Thor::Error, "No such job: #{job_config}" unless File.exists?("lib/#{job_config}.rb")
    end

    def relative_path(path)
      path.sub(Dir.pwd + '/', '')
    end

    def check_local_output!(path)
      if File.exists?(path)
        raise Thor::Error, "#{options[:output]} already exists!"
      end
    end

    def launch_local!
      output_path = options[:output] || "data/#{job_config}/output"
      output_path_parent = File.dirname(output_path)
      if options.cleanup_before?
        say("Cleaning up #{output_path}")
        remove_file(output_path)
      else
        check_local_output!(output_path)
      end
      unless File.exists?(output_path_parent)
        say("Creating #{output_path_parent}")
        empty_directory(output_path_parent)
      end
      input_glob = File.join(options[:local_completes], options[:input])
      config_path = File.expand_path('../../../config/hadoop-local.xml', __FILE__)
      system('hadoop', 'jar', project_jar, '-conf', config_path, job_config, input_glob, output_path)
    end

    def launch_emr!
      flow = EmrFlow.new(job_config, options[:input_glob], job_package, emr, job_bucket, data_bucket)
      if options.cleanup_before?
        say("Cleaning up #{flow.output_uri}")
        flow.cleanup!
      end
      say("Uploading JAR to #{flow.jar_uri}")
      flow.prepare!
      # job_flow_id = flow.launch!
      # say("Started EMR job flow #{job_flow_id}")
    end
  end
end