#!/bin/bash

if [ -e /home/hadoop/lib/jruby-complete-no-joda-1.6.5.jar ]
  then
    rm /home/hadoop/lib/jruby-complete-no-joda-1.6.5.jar
fi

if [ -e /home/hadoop/lib/jruby-complete-1.6.8.jar ]
  then
    rm /home/hadoop/lib/jruby-complete-1.6.8.jar
fi
