# encoding: utf-8

module Humboldt
  module Patterns
    class SumReducer < Reducer
      input :text, :long
      output :text, :long

      reduce do |key, values|
        sum = 0
        values.each { |v| sum += v }
        emit(key, sum)
      end
    end
  end
end