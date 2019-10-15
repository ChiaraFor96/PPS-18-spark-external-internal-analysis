#!/bin/bash

/etc/init.d/ssh start

$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

i=0
echo "date, value" >> data.csv
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal -f data.csv /
while :
do
  ((i+=1))
  echo "$(date), $i" | $HADOOP_HOME/bin/hdfs dfs -appendToFile - data.csv
  sleep 10
done

tail -f /dev/null