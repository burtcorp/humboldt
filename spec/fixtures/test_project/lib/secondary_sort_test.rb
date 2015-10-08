# encoding: utf-8

require 'humboldt'

module SecondarySortTest
  USER_ID_LENGTH = 5

  class Mapper < Humboldt::Mapper
    input :long, :text
    output :text, :text

    map do |_, value|
      site, user_id = value.split(',')
      emit("#{site}#{user_id}", user_id)
    end
  end

  class Reducer < Humboldt::Reducer
    input :text, :text
    output :text, :text

    reduce do |key, values|
      count = 0
      site = key[0, key.bytesize - USER_ID_LENGTH]
      last_value = nil
      values.each do |value|
        count += 1 if value != last_value
        last_value = value
      end
      emit(site, count.to_s)
    end
  end
end

Rubydoop.configure do |input_path, output_path|
  job 'test secondary sort' do
    input input_path
    output output_path

    mapper SecondarySortTest::Mapper
    reducer SecondarySortTest::Reducer

    secondary_sort 0, -SecondarySortTest::USER_ID_LENGTH - 1
  end
end
