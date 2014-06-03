# Humboldt

Humboldt provides a tool-set on top of Rubydoop to run Hadoop jobs
effortlessly both locally and on Amazon EMR. There is also some sugar
added on top of the Rubydoop DSL.

## Sugar

### Type converters

Humboldt adds a number of type converters:

* binary - `String` - `Hadoop::Io::BytesWritable`
* encoded - `String` - MessagePack encoded `Hadoop::Io::BytesWritable`
* text - `String` - `Hadoop::Io::Text`
* json - `Hash` - `Hadoop::Io::Text`
* long - `Integer` - `Hadoop::Io::LongWritable`
* none - `nil` - `Hadoop::Io::NullWritable`

Use them like so:

```ruby
class Mapper < Humboldt::Mapper
  input :long, :json
  # ...
end
```

### Combine input files

Hadoop does not perform well with many small input files, since each
file is handled by its own map task, by default. Humboldt bundles an
input format which combines files. Due to a bug in Hadoop 1.0.3 (and
other versions), Hadoop 2.2.0 is required to run this for input files
on S3, see
[this bug](https://issues.apache.org/jira/browse/MAPREDUCE-1806).

Example usage:

```ruby
Rubydoop.configure do |input_paths, output_path|
  job 'my job' do
    input input_paths, format: :combined_text
    set 'mapreduce.input.fileinputformat.split.maxsize', 32 * 1024 * 1024

    # ...
  end
end
```

`mapreduce.input.fileinputformat.split.maxsize` controls the maximum
size of an input split.

## Development setup

Download Hadoop and set up the classpath using

    $ rake setup

The default is Hadoop 1.0.3. Specify a hadoop release by setting
`$HADOOP_RELEASE`, e.g.

    $ HADOOP_RELEASE=hadoop-2.2.0/hadoop-2.2.0 rake setup

Run the tests with

    $ rake spec

## Release a new gem

Bump the version number in `lib/humboldt/version.rb`, run `rake gem:release`.

# Copyright

© 2014 Burt AB, see LICENSE.txt (BSD 3-Clause).