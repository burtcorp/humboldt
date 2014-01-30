module CombinedTextTest
  class Mapper < Humboldt::Mapper
    input :long, :text
    output :text, :text

    setup do
      puts "!!! combined mapper setup"
    end

    map do |key, value|
      emit('key', value)
    end
  end

  class Reducer < Humboldt::Reducer
    input :text, :text
    output :text, :text

    reduce do |key, values|
      emit key, values.to_a.join(' ')
    end
  end
end
