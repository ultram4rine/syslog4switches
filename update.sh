#!/bin/bash

go build server.go
mv -f server /usr/local/sbin/syslog4switches/
cp conf.json /usr/local/sbin/syslog4switches/
systemctl restart syslog4switches