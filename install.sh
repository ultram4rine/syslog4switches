#!/bin/bash

cp syslog4switches.service /etc/systemd/system/
systemctl daemon-reload
go build server.go
mkdir /usr/local/sbin/syslog4switches
mv -f server /usr/local/sbin/syslog4switches/
cp syslog4switches.conf.json /usr/local/sbin/syslog4switches/
systemctl enable syslog4switches
systemctl start syslog4switches