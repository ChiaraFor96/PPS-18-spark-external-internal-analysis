#!/bin/bash
#TODO PORT, HOST=`hostname --fqdn`
i=0
while true; do
  ((i+=1))
  echo -e "date: $(date);value: $i \r\n" | nc -l 9999
  sleep 0.1
done

tail -f /dev/null