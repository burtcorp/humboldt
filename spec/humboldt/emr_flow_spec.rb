# encoding: utf-8

require_relative '../spec_helper'


module Humboldt
  describe EmrFlow do
    let :emr do
      stub(:emr, :job_flows => stub(:job_flows))
    end

    let :remote_jar_file do
      stub(:remote_jar_file)
    end

    let :remote_bootstrap_file do
      stub(:remote_bootstrap_file)
    end

    let :bucket_objects do
      bo = stub(:bucket_objects)
      bo.stub(:[]).with('my_awesome_job/my_awesome_job.jar').and_return(remote_jar_file)
      bo.stub(:[]).with('my_awesome_job/config/emr-bootstrap/remove_old_jruby.sh').and_return(remote_bootstrap_file)
      bo
    end

    let :job_bucket do
      stub(:job_bucket, :objects => bucket_objects, :name => 'job-bucket')
    end

    let :data_bucket do
      stub(:data_bucket, :name => 'data-bucket')
    end

    let :package do
      stub(:package, :jar_path => 'build/my_awesome_job.jar', :project_name => 'my_awesome_job')
    end

    let :flow do
      described_class.new('some_job', 'input_glob/*/*', package, emr, data_bucket, job_bucket)
    end

    describe '#prepare!' do
      before do
        remote_jar_file.stub(:write)
        remote_bootstrap_file.stub(:write)
      end

      it 'uploads the JAR to S3' do
        remote_jar_file.should_receive(:write).with(Pathname.new(package.jar_path))
        flow.prepare!
      end

      it 'uploads bootstrap task scripts to S3' do
        remote_bootstrap_file.should_receive(:write) do |path|
          path.to_s.should end_with('config/emr-bootstrap/remove_old_jruby.sh')
        end
        flow.prepare!
      end
    end

    describe '#cleanup!' do
      it 'deletes the output directory' do
        output_objects = stub(:output_objects)
        output_objects.should_receive(:delete_all)
        bucket_objects.stub(:with_prefix).with('my_awesome_job/some_job/output').and_return(output_objects)
        flow.cleanup!
      end
    end

    describe '#output_path' do
      it 'defaults to an output path based on the project and job name' do
        flow.output_path.should == 'my_awesome_job/some_job/output'
      end

      it 'can be overridden' do
        f = described_class.new('some_job', 'input_glob/*/*', package, emr, data_bucket, job_bucket, 'some/other/path')
        f.output_path.should == 'some/other/path'
      end
    end

    describe '#run!' do
      before do
        bucket_objects.stub(:with_prefix).with('my_awesome_job/my_awesome_job.jar').and_return(['my_awesome_job.jar'])
        bucket_objects.stub(:with_prefix).with('my_awesome_job/some_job/output').and_return([])
      end

      it 'raises an error if the job JAR does not exist' do
        bucket_objects.stub(:with_prefix).with('my_awesome_job/my_awesome_job.jar').and_return([])
        expect { flow.run! }.to raise_error(%r{s3://job-bucket/my_awesome_job/my_awesome_job\.jar})
      end

      it 'raises an error if the output directory already exists' do
        bucket_objects.stub(:with_prefix).with('my_awesome_job/some_job/output').and_return(['my_awesome_job/some_job/output'])
        expect { flow.run! }.to raise_error(%r{s3://job-bucket/my_awesome_job/some_job/output})
      end

      it 'creates a new job flow' do
        emr.job_flows.should_receive(:create) do |name, configuration|
          name.should == 'my_awesome_job'
          configuration.should have_key(:log_uri)
          configuration.should have_key(:instances)
          configuration.should have_key(:steps)
          configuration.should have_key(:bootstrap_actions)
        end
        flow.run!
      end

      context 'bootstrap tasks' do
        before do
          emr.job_flows.stub(:create) do |name, configuration|
            @configuration = configuration
          end
        end

        it 'adds a bootrap task for removing old JRuby JARs' do
          flow.run!
          action = @configuration[:bootstrap_actions].find { |action| action[:name] == 'remove_old_jruby' }
          action[:script_bootstrap_action][:path].should == 's3://job-bucket/my_awesome_job/config/emr-bootstrap/remove_old_jruby.sh'
        end

        it 'adds a bootrap task for configuring Hadoop' do
          flow.run!
          action = @configuration[:bootstrap_actions].find { |action| action[:name] == 'configure_hadoop' }
          action[:script_bootstrap_action].should have_key(:path)
          action[:script_bootstrap_action].should have_key(:args)
        end
      end

      context 'job configuration' do
        before do
          emr.job_flows.should_receive(:create) do |name, configuration|
            @configuration = configuration
          end
        end

        it 'sets the log URI of the job flow' do
          flow.run!
          @configuration[:log_uri].should == 's3://job-bucket/my_awesome_job/some_job/logs'
        end

        it 'configures the job flow steps' do
          flow.run!
          @configuration[:steps].should == [{
            :name => 'my_awesome_job',
            :hadoop_jar_step => {
              :jar => 's3://job-bucket/my_awesome_job/my_awesome_job.jar',
              :args => [
                'some_job',
                's3n://data-bucket/input_glob/*/*',
                's3n://job-bucket/my_awesome_job/some_job/output'
              ]
            }
          }]
        end

        describe 'with extra job arguments' do
          let :flow do
            described_class.new('some_job', 'input_glob/*/*', package, emr, data_bucket, job_bucket, 'my_awesome_job/some_job/output')
          end

          it 'configures the job flow steps to include the extra arguments' do
            flow.run!(extra_hadoop_args: ['foo', 'bar'])
            @configuration[:steps][0][:hadoop_jar_step][:args][3..4].should == ['foo', 'bar']
          end
        end

        it 'configures the instances' do
          flow.run!
          @configuration[:instances].should == {
            :ec2_key_name => 'burt-id_rsa-gsg-keypair',
            :hadoop_version => '1.0.3',
            :instance_groups => [
              {
                :name => 'Master Group',
                :instance_role => 'MASTER',
                :instance_count => 1,
                :instance_type => 'm1.small',
                :market => 'ON_DEMAND'
              },
              {
                :name => 'Core Group',
                :instance_role => 'CORE',
                :instance_count => 4,
                :instance_type => 'c1.xlarge',
                :market => 'ON_DEMAND'
              }
            ]
          }
        end

        it 'allows changes to the instance configuration' do
          flow.run!(instance_count: 8, instance_type: 'cc2.8xlarge')
          master_group, core_group = @configuration[:instances][:instance_groups]
          core_group[:instance_count].should == 8
          core_group[:instance_type].should == 'cc2.8xlarge'
          master_group[:instance_count].should == 1
          master_group[:instance_type].should == 'm1.small'
        end

        it 'allows using an on demand instance as master node' do
          flow.run!(:spot_instances=> ['core'])
          master_group, core_group = @configuration[:instances][:instance_groups]
          master_group[:market].should == 'ON_DEMAND'
          master_group.should_not have_key(:bid_price)
          core_group[:market].should == 'SPOT'
          core_group[:bid_price].should == '0.2'
        end

        describe EmrFlow::InstanceGroupConfiguration do
          describe '.create' do
            context 'when :spot_instances is not present' do
              it 'configures all instances as on demand instances' do
                flow.run!
                master_group, core_group = @configuration[:instances][:instance_groups]
                master_group[:market].should == 'ON_DEMAND'
                master_group.should_not have_key(:bid_price)
                core_group[:market].should == 'ON_DEMAND'
                core_group.should_not have_key(:bid_price)
              end
            end

            context 'when :spot_instances is an empty list' do
              it 'configures all instance groups as spot instances' do
                flow.run!(:spot_instances => [])
                master_group, core_group = @configuration[:instances][:instance_groups]
                master_group[:market].should == 'SPOT'
                core_group[:market].should == 'SPOT'
              end

              it 'respects bid_price' do
                flow.run!(:spot_instances => [], :bid_price => '0.5')
                master_group, core_group = @configuration[:instances][:instance_groups]
                master_group[:bid_price].should == '0.5'
                core_group[:bid_price].should == '0.5'
              end
            end

            context 'when :spot_instances is a non-empty list' do
              it 'configures groups present in list as spot instances' do
                flow.run!(:spot_instances => ['core'])
                master_group, core_group = @configuration[:instances][:instance_groups]
                core_group[:market].should == 'SPOT'
              end

              it 'respects bid_price for spot instances' do
                flow.run!(:spot_instances => ['core'], :bid_price => '0.001')
                master_group, core_group = @configuration[:instances][:instance_groups]
                core_group[:bid_price].should == '0.001'
              end

              it 'configures groups not present in list as on demand instances' do
                flow.run!(:spot_instances => ['core'])
                master_group, core_group = @configuration[:instances][:instance_groups]
                master_group[:market].should == 'ON_DEMAND'
                master_group.should_not have_key(:bid_price)
              end
            end
          end
        end
      end
    end
  end
end
