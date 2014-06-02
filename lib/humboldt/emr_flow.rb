# encoding: utf-8

module Humboldt
  class EmrFlow
    attr_reader :output_path

    def initialize(*args)
      @job_name, @input_glob, @package, @emr, @data_bucket, @job_bucket, @output_path = args
      @output_path ||= "#{@package.project_name}/#{@job_name}/output"
    end

    def prepare!
      upload_bootstrap_task_files!
      upload_jar!
    end

    def cleanup!
      delete_output_dir!
    end

    def run!(launch_options={})
      check_jar!
      check_output_dir!
      create_flow!(launch_options)
    end

    def jar_path
      "#{@package.project_name}/#{File.basename(@package.jar_path)}"
    end

    def jar_uri
      s3_uri(jar_path)
    end

    def output_uri
      s3_uri(output_path)
    end

    def log_path
      "#{@package.project_name}/#{@job_name}/logs"
    end

    private

    BOOTSTRAP_TASK_FILES = {
      :remove_old_jruby => 'config/emr-bootstrap/remove_old_jruby.sh'
    }.freeze

    def s3_uri(path, options={})
      protocol = options[:protocol] || 's3'
      bucket = options[:bucket] || @job_bucket
      "#{protocol}://#{bucket.name}/#{path}"
    end

    def upload_bootstrap_task_files!
      BOOTSTRAP_TASK_FILES.values.each do |local_path|
        remote_obj = @job_bucket.objects["#{@package.project_name}/#{local_path}"]
        remote_obj.write(Pathname.new(File.expand_path(local_path, "#{__FILE__}/../../..")))
      end
    end

    def upload_jar!
      # TODO: upload only if not exists and MD5 != ETag
      jar_obj = @job_bucket.objects[jar_path]
      jar_obj.write(Pathname.new(@package.jar_path))
    end

    def check_jar!
      unless @job_bucket.objects.with_prefix(jar_path).any?
        raise "Job JAR missing (#{s3_uri(jar_path)}"
      end
    end

    def check_output_dir!
      if @job_bucket.objects.with_prefix(output_path).any?
        raise "Output directory already exists (#{s3_uri(output_path)})"
      end
    end

    def delete_output_dir!
      @job_bucket.objects.with_prefix(output_path).delete_all
    end

    def job_flow_configuration(launch_options)
      {
        :log_uri => s3_uri(log_path),
        :instances => instance_configuration(launch_options),
        :steps => [step_configuration(launch_options)],
        :bootstrap_actions => bootstrap_actions,
        :visible_to_all_users => true
      }
    end

    def instance_configuration(launch_options)
      {
        :ec2_key_name => launch_options[:ec2_key_name],
        :hadoop_version => launch_options[:hadoop_version],
        :instance_groups => InstanceGroupConfiguration.create(launch_options)
      }
    end

    def bootstrap_actions
      remove_old_jruby_action = {
        :name => 'remove_old_jruby',
        :script_bootstrap_action => {
          :path => s3_uri("#{@package.project_name}/#{BOOTSTRAP_TASK_FILES[:remove_old_jruby]}")
        }
      }

      # http://hadoop.apache.org/docs/r1.0.3/mapred-default.html
      configure_hadoop_action = {
        :name => 'configure_hadoop',
        :script_bootstrap_action => {
          :path => 's3://eu-west-1.elasticmapreduce/bootstrap-actions/configure-hadoop',
          :args => [
            '-m', 'mapred.job.reuse.jvm.num.tasks=-1',
            '-m', 'mapred.map.tasks.speculative.execution=false',
            '-m', 'mapred.reduce.tasks.speculative.execution=false'
          ]
        }
      }

      [remove_old_jruby_action, configure_hadoop_action]
    end

    def step_configuration(launch_options)
      {
        :name => @package.project_name,
        :hadoop_jar_step => {
          :jar => s3_uri(jar_path),
          :args => [
            @job_name,
            s3_uri(@input_glob, protocol: 's3n', bucket: @data_bucket),
            s3_uri(output_path, protocol: 's3n'),
            *launch_options[:extra_hadoop_args]
          ]
        }
      }
    end

    def create_flow!(launch_options)
      job_flow = @emr.job_flows.create(@package.project_name, job_flow_configuration(launch_options))
    end

    module InstanceGroupConfiguration
      extend self

      # TODO: add 'task' group when support is added for 'tasks'
      INSTANCE_GROUPS = %w[master core].freeze
      MASTER_INSTANCE_TYPE = 'm1.small'.freeze
      DEFAULT_CORE_INSTANCE_TYPE = 'c1.xlarge'.freeze
      DEFAULT_BID_PRICE = '0.2'.freeze
      DEFAULT_CORE_INSTANCE_COUNT = 4

      INSTANCE_TYPE_MAPPINGS = {
        'master' => MASTER_INSTANCE_TYPE,
        'core'   => DEFAULT_CORE_INSTANCE_TYPE
      }.freeze

      INSTANCE_COUNT_MAPPINGS = {
        'master' => 1,
        'core' => DEFAULT_CORE_INSTANCE_COUNT
      }.freeze

      def base_configuration(group)
        {:name => "#{group.capitalize} Group", :instance_role => group.upcase}
      end

      def configure_type_and_count(group, configuration, options = {})
        if group == 'core'
          configuration[:instance_type] = options[:instance_type]
          configuration[:instance_count] = options[:instance_count]
        end

        configuration[:instance_type] ||= INSTANCE_TYPE_MAPPINGS[group]
        configuration[:instance_count] ||= INSTANCE_COUNT_MAPPINGS[group]
      end

      def configure_market(group, configuration, spot_instances, bid_price)
        if spot_instances && (spot_instances.empty? || spot_instances.include?(group))
          configuration[:market] = 'SPOT'
          configuration[:bid_price] = bid_price || DEFAULT_BID_PRICE
        else
          configuration[:market] = 'ON_DEMAND'
        end
      end

      def create(options)
        INSTANCE_GROUPS.map do |group|
          configuration = base_configuration(group)
          configure_type_and_count(group, configuration, options)
          configure_market(group, configuration, options[:spot_instances], options[:bid_price])
          configuration
        end
      end
    end
  end
end
