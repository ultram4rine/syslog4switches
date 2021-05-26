package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"git.sgu.ru/ultramarine/syslog4switches/conf"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/soniah/gosnmp"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

type switchLog struct {
	IP        string
	TimeStamp time.Time
	Facility  uint8
	Severity  uint8
	Priority  uint8
	Message   string
}

type nginxLog struct {
	Hostname  string
	TimeStamp time.Time
	Facility  uint8
	Severity  uint8
	Priority  uint8
	Message   string
}

type postfixLog struct {
	Daemon    string
	TimeStamp time.Time
	Message   string
}

var confname = kingpin.Flag("conf", "Path to config file.").Short('c').Default("syslog4switches.conf").String()

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
		switchQuery  = "INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?)"
		nginxQuery   = "INSERT INTO nginx (hostname, timestamp, facility, severity, priority, message) VALUES (?, ?, ?, ?, ?, ?)"
		postfixQuery = "INSERT INTO postfix (daemon, timestamp, message) VALUES (?, ?, ?)"
	)

	var IPNameMap = make(map[string]string)

	go func(channel syslog.LogPartsChannel) {
		for logmap := range channel {
			log.Infof("Received log from %v", logmap["client"])

			switch {
			case logmap["tag"] == "nginx":
				{
					l := parseNginxLog(logmap)

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
			case strings.Contains(fmt.Sprintf("%v", logmap["tag"]), "postfix/"):
				{
					l := parsePostfixLog(logmap)

					tx, err := db.Begin()
					if err != nil {
						log.Warnf("Error starting transaction: %s", err)
					}

					_, err = tx.Exec(postfixQuery, l.Daemon, l.TimeStamp, l.Message)
					if err != nil {
						log.Warnf("Error inserting postfix log to database: %s", err)

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
			case logmap["tag"] == "":
				{
					if name, l, err := parseSwitchLog(logmap, IPNameMap); err != nil {
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

func parsePostfixLog(logmap format.LogParts) postfixLog {
	var (
		l  postfixLog
		ok bool
	)

	for k, v := range logmap {
		switch k {
		case "tag":
			{
				tag, ok := v.(string)
				if !ok {
					log.Warnf("tag wrong type")
				}
				tagArr := strings.Split(tag, "/")
				if len(tagArr) < 2 {
					l.Daemon = tagArr[0]
				} else {
					l.Daemon = tagArr[1]
				}
			}
		case "timestamp":
			l.TimeStamp, ok = v.(time.Time)
			if !ok {
				log.Warnf("timestamp wrong type")
			}
		case "content":
			l.Message, ok = v.(string)
			if !ok {
				log.Warnf("content wrong type")
			}
		}
	}

	return l
}

func parseNginxLog(logmap format.LogParts) nginxLog {
	var (
		l  nginxLog
		ok bool
	)

	for key, val := range logmap {
		switch key {
		case "content":
			{
				l.Message, ok = val.(string)
				if !ok {
					log.Warnf("content wrong type")
				}
			}
		case "hostname":
			{
				l.Hostname, ok = val.(string)
				if !ok {
					log.Warnf("hostname wrong type")
				}
			}
		case "timestamp":
			{
				l.TimeStamp, ok = val.(time.Time)
				if !ok {
					log.Warnf("timestamp wrong type")
				}
			}
		case "facility":
			{
				l.Facility, ok = val.(uint8)
				if !ok {
					log.Warnf("facility wrong type")
				}
			}
		case "severity":
			{
				l.Severity, ok = val.(uint8)
				if !ok {
					log.Warnf("severity wrong type")
				}
			}
		case "priority":
			{
				l.Priority, ok = val.(uint8)
				if !ok {
					log.Warnf("priority wrong type")
				}
			}
		}
	}

	return l
}

func parseSwitchLog(logmap format.LogParts, IPNameMap map[string]string) (string, switchLog, error) {
	var (
		l  switchLog
		ok bool
	)

	for key, val := range logmap {
		switch key {
		case "content":
			{
				l.Message, ok = val.(string)
				if !ok {
					log.Warnf("content wrong type")
				}
			}
		case "client":
			{
				ip, ok := val.(string)
				if !ok {
					log.Warnf("client wrong type")
				}
				l.IP = strings.Split(ip, ":")[0]
			}
		case "timestamp":
			{
				l.TimeStamp, ok = val.(time.Time)
				if !ok {
					log.Warnf("timestamp wrong type")
				}
			}
		case "facility":
			{
				l.Facility, ok = val.(uint8)
				if !ok {
					log.Warnf("facility wrong type")
				}
			}
		case "severity":
			{
				l.Severity, ok = val.(uint8)
				if !ok {
					log.Warnf("severity wrong type")
				}
			}
		case "priority":
			{
				l.Priority, ok = val.(uint8)
				if !ok {
					log.Warnf("priority wrong type")
				}
			}
		}
	}

	name, ok := IPNameMap[l.IP]
	if !ok {
		var err error
		name, err = getSwitchName(l.IP)
		if err != nil {
			return "", switchLog{}, err
		}
	}

	return name, l, nil
}

func getSwitchName(ip string) (name string, err error) {
	const sysName = ".1.3.6.1.2.1.1.5.0"

	sw := gosnmp.Default
	sw.Target = ip
	sw.Retries = 2

	if err := sw.Connect(); err != nil {
		return "", err
	}
	defer sw.Conn.Close()

	oids := []string{sysName}
	result, err := sw.Get(oids)
	if err != nil {
		return "", err
	}

	for _, v := range result.Variables {
		switch v.Name {
		case sysName:
			name = v.Value.(string)
		default:
			return "", errors.New("something went wrong :(")
		}
	}

	return name, nil
}
