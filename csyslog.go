package main

import (
	"git.sgu.ru/ultramarine/custom_syslog/server"

	_ "github.com/ClickHouse/clickhouse-go"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/mcuadros/go-syslog.v2"
)

var confname = kingpin.Flag("conf", "Path to config file.").Short('c').Default("csyslog.conf").String()

func main() {
	kingpin.Parse()

	var s server.Server
	if err := s.Init(confname); err != nil {
		log.Fatalf("failed to init server: %v", err)
	}

	logsChan := make(syslog.LogPartsChannel, 1000)
	handler := syslog.NewChannelHandler(logsChan)
	s.SyslogServer = syslog.NewServer()
	s.SyslogServer.SetFormat(syslog.Automatic)
	s.SyslogServer.SetHandler(handler)
	if err := s.SyslogServer.ListenUDP(":514"); err != nil {
		log.Fatalf("failed to set syslog server listen for UDP: %v", err)
	}
	if err := s.SyslogServer.Boot(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	go func(channel syslog.LogPartsChannel) {
		for logmap := range channel {
			log.Infof("Received log from %v", logmap["client"])

			if err := s.ProcessLog(logmap); err != nil {
				log.Error(err)
				continue
			}
		}
	}(logsChan)

	s.SyslogServer.Wait()
}
