#!/bin/bash
#TODO PORT, HOST=`hostname --fqdn`
while true; do
  echo -e "date: $(date);value: 100 \r\n" | nc -l -p 9999
  sleep 0.1
done

tail -f /dev/null
#TODO $(jot -r 1 0 100)