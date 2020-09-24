package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"github.com/soniah/gosnmp"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

var config struct {
	Port    string `toml:"listen_port"`
	Network string `toml:"switch_network"`
	DB      db     `toml:"db"`
}

type db struct {
	Host string `toml:"host"`
	Name string `toml:"name"`
	User string `toml:"user"`
	Pass string `toml:"pass"`
}

type switchLog struct {
	SwName       string
	SwIP         string
	LogTimeStamp time.Time
	LogFacility  uint8
	LogSeverity  uint8
	LogPriority  uint8
	LogMessage   string
}

var confpath = kingpin.Flag("conf", "Path to config file.").Short('c').Default("syslog4switches.conf.toml").String()

func main() {
	kingpin.Parse()

	if _, err := toml.DecodeFile(*confpath, &config); err != nil {
		log.Fatalf("Error decoding config file from %s", *confpath)
	}

	db, err := sqlx.Connect("clickhouse", fmt.Sprintf("%s?username=%s&password=%s&database=%s", config.DB.Host, config.DB.User, config.DB.Pass, config.DB.Name))
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

	_, network, err := net.ParseCIDR(config.Network)
	if err != nil {
		log.Fatalf("Error parsing switch network: %s", err)
	}

	const (
		entPhysicalName = ".1.3.6.1.2.1.47.1.1.1.1.7"
		query           = "INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?)"
	)
	var IPNameMap = make(map[string]string)

	go func(channel syslog.LogPartsChannel) {
		for logmap := range channel {
			if l, err := parseLog(logmap, network, entPhysicalName, IPNameMap); err == nil {
				if l.SwName == "no name" {
					log.Printf("Can't get name for %s switch", l.SwIP)
				}
				IPNameMap[l.SwIP] = l.SwName

				tx, err := db.Begin()
				if err != nil {
					log.Printf("Error starting transaction: %s", err)
					continue
				}

				_, err = tx.Exec(query, time.Now().In(loc), l.SwName, net.ParseIP(l.SwIP), l.LogTimeStamp, l.LogFacility, l.LogSeverity, l.LogPriority, l.LogMessage)
				if err != nil {
					log.Printf("Error inserting log to database: %s", err)

					err = tx.Rollback()
					if err != nil {
						log.Printf("Error aborting transaction: %s", err)
					}
				} else {
					err = tx.Commit()
					if err != nil {
						log.Printf("Error commiting transaction: %s", err)
					}
				}
			}
		}
	}(channel)

	server.Wait()
}

func parseLog(logmap format.LogParts, network *net.IPNet, entPhysicalName string, IPNameMap map[string]string) (switchLog, error) {
	var l switchLog

	for key, val := range logmap {
		switch key {
		case "content":
			l.LogMessage = val.(string)
		case "client":
			{
				l.SwIP = strings.Split(val.(string), ":")[0]
				if !network.Contains(net.ParseIP(l.SwIP)) {
					return l, fmt.Errorf("ip not in switch network")
				}
			}
		case "timestamp":
			l.LogTimeStamp = val.(time.Time)
		case "facility":
			l.LogFacility = uint8(val.(int))
		case "severity":
			l.LogSeverity = uint8(val.(int))
		case "priority":
			l.LogPriority = uint8(val.(int))
		}
	}

	if _, ok := IPNameMap[l.SwIP]; !ok {
		sw := gosnmp.Default

		sw.Target = l.SwIP
		sw.Retries = 2

		if err := sw.Connect(); err != nil {
			l.SwName = "no name"
		}
		defer sw.Conn.Close()

		oids := []string{entPhysicalName}
		result, err := sw.Get(oids)
		if err != nil {
			l.SwName = "no name"
		}

		for _, v := range result.Variables {
			switch v.Name {
			case entPhysicalName:
				l.SwName = string(v.Value.([]byte))
			default:
				l.SwIP = "no name"
			}
		}
	}

	return l, nil
}
