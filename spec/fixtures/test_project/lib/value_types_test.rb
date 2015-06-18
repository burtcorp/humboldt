module ValueTypesTest
  LibraryKey = Humboldt::Key.new(:shelf, :section)
  LibraryValue = Humboldt::Value.new(:book_count)

  class Mapper < Humboldt::Mapper
    input :long, :text
    output LibraryKey, LibraryValue

    map do |_, line|
      shelf, section = line.split('/')
      emit LibraryKey.new(shelf, section), LibraryValue.new(1)
    end
  end

  class Reducer < Humboldt::Reducer
    input LibraryKey, LibraryValue
    output :text, :text

    reduce do |key, values|
      output_key = [key.shelf, key.section].join('.')
      output_value = values.map(&:book_count).reduce(&:+).to_s
      emit output_key, output_value
    end
  end
end
