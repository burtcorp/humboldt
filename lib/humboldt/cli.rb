# encoding: utf-8

require 'thor'
require 'aws'
require 'rubydoop/version'
require 'rubydoop/package' # this prints an annoying warning in JRuby 1.7.0.RC1
require 'humboldt/emr_flow'
require 'humboldt/hadoop_status_filter'



module Humboldt
  class Cli < Thor
    include Thor::Actions

    DEFAULTS = {
      data_path: 'data/completes',
      silent: true,
      skip_package: false,
      extra_hadoop_args: [],
      cleanup_before: false,
      instance_count: 4,
      instance_type: 'c1.xlarge',
      spot_instances: nil,
      bid_price: 0.2,
      poll: false,
      skip_prepare: false,
      aws_region: 'eu-west-1',
      hadoop_version: '1.0.3'
    }

    desc 'package', 'Package job JAR file'
    def package
      say_status(:package, relative_path(job_package.jar_path))
      job_package.create!
    end

    desc 'run-local', 'run a job in local mode with the hadoop command'
    method_option :input, :type => :string, :required => true, :desc => 'input glob, will be resolved agains the data path'
    method_option :output, :type => :string, :desc => 'the output directory, defaults to "data/<job_config>/output"'
    method_option :job_config, :type => 'string', :desc => 'the name of the Ruby file containing the job configuration, defaults to the project name (e.g. "lib/<job_config>.rb")'
    method_option :hadoop_config, :type => 'string', :desc => 'the path to a Hadoop configuration XML file, defaults to Humboldt-provided config that runs Hadoop in local-mode'
    method_option :cleanup_before, :type => :boolean, :desc => "automatically remove the output dir before launching (default: #{DEFAULTS[:cleanup_before]})"
    method_option :data_path, :type => :string, :desc => "input paths will be resolved against this path (default: #{DEFAULTS[:data_path]})"
    method_option :silent, :type => :boolean, :desc => "silence the hadoop command's logging (default: #{DEFAULTS[:silent]})"
    method_option :skip_package, :type => :boolean, :desc => "don't package the JAR, use only if you haven't changed anything since the last run (default: #{DEFAULTS[:skip_package]})"
    method_option :extra_hadoop_args, :type => :array, :desc => "extra arguments to on pass to hadoop (default: #{DEFAULTS[:extra_hadoop_args]})"
    def run_local
      check_job!
      invoke(:package, [], {}) unless options.skip_package?
      output_path = options[:output] || "data/#{job_config}/output"
      output_path_parent = File.dirname(output_path)
      if options.cleanup_before?
        remove_file(output_path)
      else
        check_local_output!(output_path)
      end
      unless File.exists?(output_path_parent)
        empty_directory(output_path_parent)
      end
      input_glob = File.join(options[:data_path], options[:input])
      hadoop_config_path = options[:hadoop_config] || default_hadoop_config_path
      run_command('hadoop', 'jar', project_jar, job_config, '-conf', hadoop_config_path, input_glob, output_path, *options[:extra_hadoop_args])
    end

    # @deprecated EMR support will be removed in 2.0
    desc 'run-emr', 'run a job in Elastic MapReduce'
    method_option :input, :type => :string, :required => true, :desc => 'input glob, will be resolved against the data bucket'
    method_option :output, :type => :string, :desc => 'the output directory, defaults to "<project_name>/<job_config>/output" in the job bucket'
    method_option :job_config, :type => 'string', :desc => 'the name of the Ruby file containing the job configuration, defaults to the project name (e.g. "lib/<job_config>.rb")'
    method_option :cleanup_before, :type => :boolean, :desc => "automatically remove the output dir before launching (default: #{DEFAULTS[:cleanup_before]})"
    method_option :data_bucket, :type => :string, :desc => "S3 bucket containing input data (default: #{DEFAULTS[:data_bucket]})"
    method_option :job_bucket, :type => :string, :desc => "S3 bucket to upload JAR, output logs and results into (default: #{DEFAULTS[:job_bucket]})"
    method_option :instance_count, :type => :numeric, :desc => "the number of worker instances to launch (default: #{DEFAULTS[:instance_count]})"
    method_option :instance_type, :type => :string, :desc => "the worker instance type, see http://ec2pricing.iconara.info/ for available types (default: #{DEFAULTS[:instance_type]})"
    method_option :spot_instances, :type => :array, :lazy_default => [], :desc => 'use spot instances; either an explicit list of instance groups or no value to run all groups as spot instances'
    method_option :bid_price, :type => :string, :desc => "how much to bid for spot instances, see http://ec2pricing.iconara.info/ for current spot prices (default: #{DEFAULTS[:bid_price]})"
    method_option :poll, :type => :boolean, :desc => "poll the job's status every 10s and display (default: #{DEFAULTS[:poll]})"
    method_option :skip_package, :type => :boolean, :desc => "don't package the JAR, use only if you haven't changed anything since the last run (default: #{DEFAULTS[:skip_package]})"
    method_option :skip_prepare, :type => :boolean, :desc => "don't upload the JAR and bootstrap files, use only if you haven't changed anything since the last run (default: #{DEFAULTS[:skip_prepare]})"
    method_option :extra_hadoop_args, :type => :array, :desc => "extra arguments to pass on to hadoop (default: #{DEFAULTS[:extra_hadoop_args]})"
    method_option :ec2_key_name, :type => :string, :desc => 'The name of an EC2 key pair to enable SSH access to master node'
    method_option :aws_region, :type => :string, :desc => "The AWS region where the EMR flow is to run (default: #{DEFAULTS[:aws_region]})"
    method_option :hadoop_version, :type => :string, :desc => "The EMR Hadoop version to use (default: #{DEFAULTS[:hadoop_version]})"
    def run_emr
      check_job!
      invoke(:package, [], {}) unless options.skip_package?
      flow = EmrFlow.new(job_config, options[:input], job_package, emr, data_bucket, job_bucket, options[:output])
      if options.cleanup_before?
        say_status(:remove, flow.output_uri)
        flow.cleanup!
      end
      unless options.skip_prepare?
        say_status(:upload, flow.jar_uri)
        flow.prepare!
      end
      say_status(:warning, "No EC2 key name configured. You will not be able to access the master node via SSH.", :yellow) unless options[:ec2_key_name]
      job_flow = flow.run!(
        bid_price: options[:bid_price],
        instance_count: options[:instance_count],
        instance_type: options[:instance_type],
        spot_instances: options[:spot_instances],
        extra_hadoop_args: options[:extra_hadoop_args],
        ec2_key_name: options[:ec2_key_name],
        hadoop_version: options[:hadoop_version]
      )
      File.open('.humboldtjob', 'w') { |io| io.puts(job_flow.job_flow_id) }
      say_status(:started, %{EMR job flow "#{job_flow.job_flow_id}"})
    end

    # @deprecated EMR support will be removed in 2.0
    desc 'emr-job', 'show status of the last EMR job'
    def emr_job
      if File.exists?('.humboldtjob')
        job_flow_id = File.read('.humboldtjob').strip
        job_flow = emr.job_flows[job_flow_id]
        print_job_flow_extended_status(job_flow)
      else
        say_status(:warning, 'Could not determine last job flow ID')
      end
    end

    # @deprecated EMR support will be removed in 2.0
    desc 'emr-jobs', 'list all EMR jobs'
    def emr_jobs
      emr.job_flows.each do |job_flow|
        print_job_flow_status(job_flow)
      end
    end

    desc 'configure', 'Configure humboldt for the current project'
    def configure
      say("Please ensure you are located at the root directory of the project you are configuring.", :yellow)
      configuration = options_from_config_file
      say('EMR configuration', :green)
      configuration[:ec2_key_name] = ask("EC2 key pair name to enable SSH access to EMR master node: [#{config_file_options_with_defaults[:ec2_key_name]}]")
      configuration[:aws_region] = ask("AWS region: [#{config_file_options_with_defaults[:aws_region]}]")
      configuration[:hadoop_version] = ask("Hadoop version: [#{config_file_options_with_defaults[:hadoop_version]}]")
      configuration[:data_bucket] = ask("Input data S3 bucket: [#{config_file_options_with_defaults[:data_bucket]}]")
      configuration[:job_bucket] = ask("Job S3 bucket (where JAR is uploaded, output logs and job output go to): [#{config_file_options_with_defaults[:job_bucket]}]")
      configuration.each do |key, value|
        value = configuration[key] = config_file_options_with_defaults[key] if value.empty?
        configuration.delete(key) if value.empty? || value == DEFAULTS[key]
      end
      File.open('.humboldt.yml', 'w') { |f| YAML.dump(configuration, f) }
      say('Updated .humboldt.yml', :green)
    end

    no_commands do
      def options
        @extended_options ||= Thor::CoreExt::HashWithIndifferentAccess.new(config_file_options_with_defaults.merge(super))
      end
    end

    private

    ISO_DATE_TIME = '%Y-%m-%d %H:%M:%S'.freeze

    def project_jar
      @project_jar ||= Dir['build/*.jar'].reject { |path| path.start_with?('build/jruby-complete') }.first
    end

    def job_package
      @job_package ||= Rubydoop::Package.new(lib_jars: Dir[File.expand_path('../../**/*.jar', __FILE__)])
    end

    def job_config
      options[:job_config] || job_package.project_name
    end

    def default_hadoop_config_path
      File.expand_path('../../../config/hadoop-local.xml', __FILE__)
    end

    def s3
      @s3 ||= AWS::S3.new
    end

    def emr
      @emr ||= AWS::EMR.new(region: options[:aws_region])
    end

    def job_bucket
      @job_bucket ||= s3.buckets[options[:job_bucket]]
    end

    def data_bucket
      @data_bucket ||= s3.buckets[options[:data_bucket]]
    end

    def check_job!
      raise Thor::Error, "No such job: #{job_config}" unless File.exists?("bin/#{job_config}")
    end

    def relative_path(path)
      path.sub(Dir.pwd + '/', '')
    end

    def check_local_output!(path)
      if File.exists?(path)
        raise Thor::Error, "#{options[:output]} already exists!"
      end
    end

    def run_command(*args)
      say_status(:running, 'Hadoop started')
      HadoopStatusFilter.run_command_with_filtering(*args) do |type, *args|
        case type
        when :stderr
          unless options.silent?
            say(args.first, :red)
          end
        when :status
          say(args[0], args[1] == :error ? :red : :yellow)
        when :counters
          say
          print_table(args.first)
          say
        when :done
          say('Job completed')
        when :failed
          say('Job failed', :red)
        else
          say(args.first)
        end
      end
    end

    def print_job_flow_extended_status(job_flow)
      id = job_flow.job_flow_id
      state = job_flow.state
      created_at = job_flow.created_at.strftime(ISO_DATE_TIME)
      change_reason = job_flow.last_state_change_reason
      say_status(:started, created_at)
      say_status(:state, state)
      say_status(:change, change_reason)
    rescue => e
      say_status(:error, e.message, :red)
      sleep 1
      retry
    end

    def print_job_flow_status(job_flow)
      id = job_flow.job_flow_id
      state = job_flow.state
      created_at = job_flow.created_at.strftime(ISO_DATE_TIME)
      change_reason = job_flow.last_state_change_reason
      say_status(:status, sprintf('%-15s %-10s %19s %s', id, state, created_at, change_reason))
    rescue => e
      say_status(:error, e.message, :red)
      sleep 1
      retry
    end

    def config_file_options_with_defaults
      @config_file_options_with_defaults ||= DEFAULTS.merge(options_from_config_file)
    end

    def options_from_config_file
      @options_from_config_file ||= begin
        ::YAML::load_file(".humboldt.yml")
      rescue Errno::ENOENT
        {}
      end
    end
  end
end
