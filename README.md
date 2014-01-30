# Humboldt

## Development setup

Download Hadoop and set up the classpath using

    $ rake setup

The default is Hadoop 1.0.3. Specify a hadoop release by setting
`$HADOOP_RELEASE`, e.g.

    $ HADOOP_RELEASE=hadoop-2.2.0/hadoop-2.2.0 rake setup

Run the tests with

    $ rake spec

_NB!_ `rspec` or `rspec spec` will not work well since all specs for
gems bundled with the test project will be run.
