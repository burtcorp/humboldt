# encoding: utf-8

require 'fileutils'
require 'rubydoop'
require 'hadoop'

$CLASSPATH << File.expand_path('../humboldt.jar', __FILE__)
Java::Humboldt::HumboldtLibrary.new.load(JRuby.runtime, false)

require 'humboldt/java_lib'

require 'ext/hadoop'
require 'ext/rubydoop'

require 'humboldt/type_converters'
require 'humboldt/processor'
require 'humboldt/mapper'
require 'humboldt/reducer'
require 'humboldt/prefix_grouping'
