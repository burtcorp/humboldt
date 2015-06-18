module Humboldt
  class Value < Struct
    def self.new(*)
      cls = super
      cls.class_eval do
        include Packable
        alias_method :to_packable, :to_a
        def self.from_packable(packed)
          new(*packed)
        end
      end
      cls
    end
  end
end
