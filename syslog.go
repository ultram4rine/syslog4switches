package main

import (
	"fmt"
	"net"
	"strings"
	"time"

	"git.sgu.ru/ultramarine/syslog4switches/conf"
	"git.sgu.ru/ultramarine/syslog4switches/parsers"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/mcuadros/go-syslog.v2"
)

var confname = kingpin.Flag("conf", "Path to config file.").Short('c').Default("syslog.conf").String()

func main() {
	kingpin.Parse()

	if err := conf.Load(*confname); err != nil {
		log.Fatalf("Failed to load configuration: %s", err)
	}

	db, err := sqlx.Connect("clickhouse", fmt.Sprintf("%s?username=%s&password=%s&database=%s", conf.Config.DBHost, conf.Config.DBUser, conf.Config.DBPass, conf.Config.DBName))
	if err != nil {
		log.Fatalf("Error connecting to database: %s", err)
	}
	defer db.Close()

	channel := make(syslog.LogPartsChannel, 1000)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	server.SetFormat(syslog.Automatic)
	server.SetHandler(handler)

	if err = server.ListenUDP(":514"); err != nil {
		log.Fatalf("Error configuring server for UDP listen: %s", err)
	}

	if err = server.Boot(); err != nil {
		log.Fatalf("Error starting server: %s", err)
	}

	loc, err := time.LoadLocation("Europe/Saratov")
	if err != nil {
		log.Fatalf("Error getting time zone: %s", err)
	}

	const (
		switchQuery = "INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?)"
		nginxQuery  = "INSERT INTO nginx (hostname, timestamp, facility, severity, priority, message) VALUES (?, ?, ?, ?, ?, ?)"
		mailQuery   = "INSERT INTO mail (service, timestamp, message) VALUES (?, ?, ?)"
	)

	var IPNameMap = make(map[string]string)

	go func(channel syslog.LogPartsChannel) {
		for logmap := range channel {
			log.Infof("Received log from %v", logmap["client"])

			tag, ok := logmap["tag"].(string)
			if !ok {
				log.Warnf("tag wrong type")
			}

			switch {
			case tag == "nginx":
				{
					l, err := parsers.ParseNginxLog(logmap)

					tx, err := db.Begin()
					if err != nil {
						log.Warnf("Error starting transaction: %s", err)
						continue
					}

					_, err = tx.Exec(nginxQuery, l.Hostname, l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message)
					if err != nil {
						log.Warnf("Error inserting nginx log to database: %s", err)

						err = tx.Rollback()
						if err != nil {
							log.Warnf("Error aborting transaction: %s", err)
						}
					} else {
						err = tx.Commit()
						if err != nil {
							log.Warnf("Error commiting transaction: %s", err)
						}
					}
				}
			case strings.Contains(tag, "postfix") || strings.Contains(tag, "dovecot"):
				{
					l, err := parsers.ParseMailLog(logmap)

					tx, err := db.Begin()
					if err != nil {
						log.Warnf("Error starting transaction: %s", err)
					}

					_, err = tx.Exec(mailQuery, l.Service, l.TimeStamp, l.Message)
					if err != nil {
						log.Warnf("Error inserting mail log to database: %s", err)

						err = tx.Rollback()
						if err != nil {
							log.Warnf("Error aborting transaction: %s", err)
						}
					} else {
						err = tx.Commit()
						if err != nil {
							log.Warnf("Error commiting transaction: %s", err)
						}
					}
				}
			case tag == "":
				{
					if name, l, err := parsers.ParseSwitchLog(logmap, IPNameMap); err != nil {
						log.Warnf("Failed to parse switch log: %s", err)
					} else {
						IPNameMap[l.IP] = name

						tx, err := db.Begin()
						if err != nil {
							log.Warnf("Error starting transaction: %s", err)
							continue
						}

						_, err = tx.Exec(switchQuery, time.Now().In(loc), name, net.ParseIP(l.IP), l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message)
						if err != nil {
							log.Warnf("Error inserting switch log to database: %s", err)

							err = tx.Rollback()
							if err != nil {
								log.Warnf("Error aborting transaction: %s", err)
							}
						} else {
							err = tx.Commit()
							if err != nil {
								log.Warnf("Error commiting transaction: %s", err)
							}
						}
					}
				}
			}
		}
	}(channel)

	server.Wait()
}
