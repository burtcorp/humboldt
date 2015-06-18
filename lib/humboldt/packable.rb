module Humboldt
  module Packable
    def to_packable
      raise '#to_packable must be overridden when including Humboldt::Packable'
    end

    module ClassMethods
      def from_packable(*)
        raise '.from_packable must be overridden when including Humboldt::Packable'
      end
    end

    def self.included(cls)
      cls.extend(ClassMethods)
    end
  end
end
