#!/bin/bash

/etc/init.d/ssh start

$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

i=0
id=0
while :
do
  ((i+=1))
  echo "id,timestamp,value" >> data${i}.csv
  for X in $(seq 5)
  do
      echo "$id,$(date +%T),$X" >> data${i}.csv
        ((id+=1))
      sleep 1
  done
  $HADOOP_HOME/bin/hdfs dfs -copyFromLocal -f data${i}.csv /
  sleep 10
done

tail -f /dev/null