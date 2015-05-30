This MapReduce program is an example of how Hadoop streaming can be used
to create MapReduce programs in Ruby. This script is a word count
program which sends keys to a certain partitioner based on the
beginning character. 

The following code can be used to run this program via Linux command line:

#!/bin/bash

HADOOP_HOME=/usr/local/hadoop
JAR=contrib/streaming/hadoop-streaming-0.20.2+737.jar

HSTREAMING="$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/$JAR"

$HSTREAMING \
 -mapper  'ruby map.rb' \
 -reducer 'ruby reduce.rb' \
 -file map.rb \
 -file reduce.rb \
 -input '/user/phil/input/*' \
 -output /user/phil/output