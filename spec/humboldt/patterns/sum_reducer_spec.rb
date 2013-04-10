# encoding: utf-8

require 'spec_helper'


module Humboldt
  module Patterns
    describe SumReducer do
      let :reducer do
        described_class.new
      end

      describe '#reduce' do
        it 'emits the input key' do
          results = run_reducer(reducer, ['foo', [1, 2, 3]], ['bar', [6, 5, 4]])
          results[0][0].should == 'foo'
          results[1][0].should == 'bar'
        end

        it 'emits the sum of the input values' do
          results = run_reducer(reducer, ['foo', [1, 2, 3]], ['bar', [6, 5, 4]])
          results[0][1].should == 6
          results[1][1].should == 15
        end
      end
    end
  end
end