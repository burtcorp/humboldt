# encoding: utf-8

require 'spec_helper'

module Humboldt
  describe Value do
    let :value_type do
      described_class.new(:genre, :year)
    end

    it 'is a Struct' do
      value_type.new.should be_a(Struct)
    end

    it 'includes Packable' do
      value_type.should include(Packable)
    end
  end
end
