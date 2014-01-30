module DistributedCacheTest
  class Mapper < Humboldt::Mapper
    input :long, :text
    output :text, :text

    setup do
      @important = File.read('important.doc')
    end

    map do |key, value|
      other = File.read('another_file')
      emit('mapper', @important + '/' + other)
    end
  end

  class Reducer < Humboldt::Reducer
    input :text, :text
    output :text, :text

    setup do
      @important = File.read('important.doc')
    end

    reduce do |key, values|
      emit key, values.next
      other = File.read('another_file')
      emit('reducer', @important + '/' + other)
    end
  end
end
