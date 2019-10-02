#!/bin/bash

cp syslog-serv.service /etc/systemd/system/
systemctl daemon-reload
go build syslog-serv.go
mv -f syslog-serv /usr/local/sbin/syslog-serv/
cp conf.json /usr/local/sbin/syslog-serv/
systemctl enable syslog-serv
systemctl start syslog-serv