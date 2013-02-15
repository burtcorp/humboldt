# encoding: utf-8

require 'spec_helper'

module Humboldt
  describe TypeConverter do
    context '.[]' do
      context 'with binary' do
        let :converter do
          TypeConverter[:binary].new
        end

        it 'stores a ruby String' do
          converter.ruby = '1234'
          converter.ruby.should == '1234'
        end

        it 'stores a hadoop BytesWritable' do
          converter.hadoop = ::Hadoop::Io::BytesWritable.new('123'.to_java_bytes)
          String.from_java_bytes(converter.hadoop.bytes)[0, converter.hadoop.length].should == '123'
        end

        it 'converts ruby to hadoop' do
          converter.ruby = '12'
          String.from_java_bytes(converter.hadoop.bytes)[0, converter.hadoop.length].should == '12'
        end

        it 'converts hadoop to ruby' do
          converter.hadoop = ::Hadoop::Io::BytesWritable.new('1'.to_java_bytes)
          converter.ruby.should == '1'
        end

        it 'complains if you try to set the ruby value to something that is not a string' do
          expect { converter.ruby = 1 }.to raise_error(/Hadoop type mismatch/)
        end

        it 'complains if you try to set the hadoop value to something that is not a BytesWritable' do
          expect { converter.hadoop = ::Hadoop::Io::Text.new('foo') }.to raise_error(/Hadoop type mismatch/)
        end
      end

      context 'with text' do
        let :converter do
          TypeConverter[:text].new
        end

        it 'stores a ruby String' do
          converter.ruby = '1234'
          converter.ruby.should == '1234'
        end

        it 'stores a hadoop Text' do
          converter.hadoop = ::Hadoop::Io::Text.new('123')
          converter.hadoop.to_s.should == '123'
        end

        it 'converts ruby to hadoop' do
          converter.ruby = '12'
          converter.hadoop.to_s.should == '12'
        end

        it 'converts hadoop to ruby' do
          converter.hadoop = ::Hadoop::Io::Text.new('1')
          converter.ruby.should == '1'
        end

        it 'handles utf-8 encoding' do
          str = "12Ä"
          str.bytesize.should == 4
          converter.ruby = str
          converter.hadoop.length.should == 4
          converter.hadoop.to_s.should == "12Ä"
        end

        it 'handles non-utf-8 encoding' do
          str = "12\xC4".force_encoding('ISO-8859-1')
          str.bytesize.should == 3
          converter.ruby = str
          converter.hadoop.length.should == 4
          converter.hadoop.to_s.should == "12Ä"
        end

        it 'complains if you try to set the ruby value to something that is not a string' do
          expect { converter.ruby = 1 }.to raise_error(/Hadoop type mismatch/)
        end

        it 'complains if you try to set the hadoop value to something that is not a Text' do
          expect { converter.hadoop = ::Hadoop::Io::LongWritable.new }.to raise_error(/Hadoop type mismatch/)
        end
      end

      context 'with long' do
        let :converter do
          TypeConverter[:long].new
        end

        it 'stores a ruby Integer' do
          converter.ruby = 1234
          converter.ruby.should == 1234
        end

        it 'stores a hadoop LongWritable' do
          converter.hadoop = ::Hadoop::Io::LongWritable.new(123)
          converter.hadoop.get.should == 123
        end

        it 'converts ruby to hadoop' do
          converter.ruby = 12
          converter.hadoop.get.should == 12
        end

        it 'converts hadoop to ruby' do
          converter.hadoop = ::Hadoop::Io::LongWritable.new(1)
          converter.ruby.should == 1
        end

        it 'complains if you try to set the ruby value to something that is not an integer' do
          expect { converter.ruby = 1.2 }.to raise_error(/Hadoop type mismatch/)
        end

        it 'complains if you try to set the hadoop value to something that is not a LongWritable' do
          expect { converter.hadoop = ::Hadoop::Io::Text.new('foo') }.to raise_error(/Hadoop type mismatch/)
        end
      end

      context 'with encoded' do
        let :converter do
          TypeConverter[:encoded].new
        end

        let :example_value do
          {'hello' => 'world', 42 => 'whee!'}
        end

        it 'stores a Ruby hash' do
          converter.ruby = example_value
          converter.ruby.should == example_value
        end

        it 'stores a Hadoop BytesWritable' do
          converter.hadoop = ::Hadoop::Io::BytesWritable.new(MessagePack.pack(example_value).to_java_bytes)
          MessagePack.unpack(String.from_java_bytes(converter.hadoop.bytes)[0, converter.hadoop.length]).should == example_value
        end

        it 'converts from Ruby to Hadoop' do
          converter.ruby = example_value
          MessagePack.unpack(String.from_java_bytes(converter.hadoop.bytes)[0, converter.hadoop.length]).should == example_value
        end

        it 'converts from Hadoop to Ruby' do
          converter.hadoop = ::Hadoop::Io::BytesWritable.new(MessagePack.pack(example_value).to_java_bytes)
          converter.ruby.should == example_value
        end

        it 'complains if you try to set the ruby value to something that is not a hash or array' do
          expect { converter.ruby = 1 }.to raise_error(/Hadoop type mismatch/)
        end

        it 'complains if you try to set the hadoop value to something that is not a BytesWritable' do
          expect { converter.hadoop = ::Hadoop::Io::Text.new('foo') }.to raise_error(/Hadoop type mismatch/)
        end
      end

      context 'with json' do
        let :converter do
          TypeConverter[:json].new
        end

        let :example_value do
          {'hello' => 'world', 'foo' => 'bar'}
        end

        it 'stores a Ruby hash' do
          converter.ruby = example_value
          converter.ruby.should == example_value
        end

        it 'stores a Hadoop BytesWritable' do
          converter.hadoop = ::Hadoop::Io::Text.new(JSON.generate(example_value))
          JSON.parse(converter.hadoop.to_s).should == example_value
        end

        it 'converts from Ruby to Hadoop' do
          converter.ruby = example_value
          JSON.parse(converter.hadoop.to_s).should == example_value
        end

        it 'converts from Hadoop to Ruby' do
          converter.hadoop = ::Hadoop::Io::Text.new(JSON.generate(example_value))
          converter.ruby.should == example_value
        end

        it 'complains if you try to set the ruby value to something that is not a string' do
          expect { converter.ruby = 1 }.to raise_error(/Hadoop type mismatch/)
        end

        it 'complains if you try to set the hadoop value to something that is not a Text' do
          expect { converter.hadoop = ::Hadoop::Io::LongWritable.new }.to raise_error(/Hadoop type mismatch/)
        end
      end

      context 'with none' do
        let :converter do
          TypeConverter[:none].new
        end

        it 'stores a Ruby nil' do
          converter.ruby = nil
          converter.ruby.should be_nil
        end

        it 'stores a Hadoop NullWritable' do
          converter.hadoop = ::Hadoop::Io::NullWritable.get
          converter.hadoop.should be(::Hadoop::Io::NullWritable.get)
        end

        it 'converts from Ruby to Hadoop' do
          converter.ruby = nil
          converter.hadoop.should be(::Hadoop::Io::NullWritable.get)
        end

        it 'converts from Hadoop to Ruby' do
          converter.hadoop = ::Hadoop::Io::NullWritable.get
          converter.ruby.should be_nil
        end

        it 'complains if you try to set the ruby value to something that is not nil' do
          expect { converter.ruby = 1 }.to raise_error(/Hadoop type mismatch/)
        end

        it 'complains if you try to set the hadoop value to something that is not a NullWritable' do
          expect { converter.hadoop = ::Hadoop::Io::Text.new('foo') }.to raise_error(/Hadoop type mismatch/)
        end
      end
    end

    context '.from_hadoop' do
      it 'returns Binary for BytesWritable' do
        TypeConverter.from_hadoop(::Hadoop::Io::BytesWritable).should == TypeConverter[:binary]
      end

      it 'returns Long for LongWritable' do
        TypeConverter.from_hadoop(::Hadoop::Io::LongWritable).should equal(TypeConverter[:long])
      end

      it 'returns Text for Text' do
        TypeConverter.from_hadoop(::Hadoop::Io::Text).should equal(TypeConverter[:text])
      end

      it 'returns None for NullWritable' do
        TypeConverter.from_hadoop(::Hadoop::Io::NullWritable).should equal(TypeConverter[:none])
      end

      it 'complains about the rest' do
        expect { TypeConverter.from_hadoop(Hadoop::Io::IntWritable) }.to raise_error(ArgumentError)
      end
    end
  end
end
