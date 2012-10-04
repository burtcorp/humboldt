# encoding: utf-8

module Humboldt
  class EmrFlow
    def initialize(*args)
      @job_name, @input_glob, @package, @emr, @job_bucket, @data_bucket = args
    end

    def prepare!
      upload_jar!
    end

    def cleanup!
      delete_output_dir!
    end

    def launch!(launch_options={})
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

    def output_path
      "#{@package.project_name}/#{@job_name}/output"
    end

    def output_uri
      s3_uri(output_path)
    end

    def log_path
      "#{@package.project_name}/#{@job_name}/logs"
    end

    private

    EC2_KEY_NAME = 'burt-id_rsa-gsg-keypair'.freeze
    HADOOP_VERSION = '1.0.3'.freeze
    MASTER_INSTANCE_TYPE = 'm1.small'.freeze
    DEFAULT_CORE_INSTANCE_TYPE = 'c1.xlarge'.freeze
    DEFAULT_BID_PRICE = '0.2'.freeze
    DEFAULT_CORE_INSTANCE_COUNT = 4

    def s3_uri(path, options={})
      protocol = options[:protocol] || 's3'
      bucket = options[:bucket] || @job_bucket
      "#{protocol}://#{bucket.name}/#{path}"
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
        :steps => [step_configuration]
      }
    end

    def instance_configuration(launch_options)
      {
        :ec2_key_name => EC2_KEY_NAME,
        :hadoop_version => HADOOP_VERSION,
        :instance_groups => [
          {
            :name => 'Master Group',
            :instance_role => 'MASTER',
            :instance_count => 1,
            :instance_type => MASTER_INSTANCE_TYPE,
            :market => 'SPOT',
            :bid_price => launch_options[:bid_price] || DEFAULT_BID_PRICE
          },
          {
            :name => 'Core Group',
            :instance_role => 'CORE',
            :instance_count => launch_options[:instance_count] || DEFAULT_CORE_INSTANCE_COUNT,
            :instance_type => launch_options[:instance_type] || DEFAULT_CORE_INSTANCE_TYPE,
            :market => 'SPOT',
            :bid_price => launch_options[:bid_price] || DEFAULT_BID_PRICE
          }
        ]
      }
    end

    def step_configuration
      {
        :name => @package.project_name,
        :hadoop_jar_step => {
          :jar => s3_uri(jar_path),
          :args => [
            @job_name,
            s3_uri(@input_glob, protocol: 's3n', bucket: @data_bucket),
            s3_uri(output_path, protocol: 's3n')
          ]
        }

      }
    end

    def create_flow!(launch_options)
      job_flow = @emr.job_flows.create(@package.project_name, job_flow_configuration(launch_options))

    end
  end
end