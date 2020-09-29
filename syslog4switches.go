package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/soniah/gosnmp"
	"github.com/spf13/viper"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

var config struct {
	Host string
	Name string
	User string
	Pass string
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

var confname = kingpin.Flag("conf", "Path to config file.").Short('c').Default("syslog4switches.conf").String()

func main() {
	kingpin.Parse()

	viper.SetConfigName(*confname)
	viper.AddConfigPath("/etc/syslog4switches/")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Warnf("Error decoding config file from %s: %s", *confname, err)
	}

	viper.SetEnvPrefix("s4s")
	if err := viper.BindEnv("switch_network"); err != nil {
		log.Warn("Failed to bind switch_network ENV variable")
	}
	if err := viper.BindEnv("db_host"); err != nil {
		log.Warn("Failed to bind db_host ENV variable")
	}
	if err := viper.BindEnv("db_name"); err != nil {
		log.Warn("Failed to bind db_name ENV variable")
	}
	if err := viper.BindEnv("db_user"); err != nil {
		log.Warn("Failed to bind db_user ENV variable")
	}
	if err := viper.BindEnv("db_pass"); err != nil {
		log.Warn("Failed to bind db_pass ENV variable")
	}

	db, err := sqlx.Connect("clickhouse", fmt.Sprintf("%s?username=%s&password=%s&database=%s", viper.GetString("db_host"), viper.GetString("db_user"), viper.GetString("db_pass"), viper.GetString("db_name")))
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

	const query = "INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?)"

	var IPNameMap = make(map[string]string)

	go func(channel syslog.LogPartsChannel) {
		for logmap := range channel {
			if l, err := parseLog(logmap, IPNameMap); err != nil {
				log.Infof("Failed to parse log: %s", err)
			} else {
				IPNameMap[l.SwIP] = l.SwName

				tx, err := db.Begin()
				if err != nil {
					log.Warnf("Error starting transaction: %s", err)
					continue
				}

				_, err = tx.Exec(query, time.Now().In(loc), l.SwName, net.ParseIP(l.SwIP), l.LogTimeStamp, l.LogFacility, l.LogSeverity, l.LogPriority, l.LogMessage)
				if err != nil {
					log.Warnf("Error inserting log to database: %s", err)

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
	}(channel)

	server.Wait()
}

func parseLog(logmap format.LogParts, IPNameMap map[string]string) (l switchLog, err error) {
	for key, val := range logmap {
		switch key {
		case "content":
			l.LogMessage = val.(string)
		case "client":
			l.SwIP = strings.Split(val.(string), ":")[0]
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

	if name, ok := IPNameMap[l.SwIP]; !ok {
		l.SwName, err = getSwitchName(l.SwIP)
		if err != nil {
			return switchLog{}, err
		}
	} else {
		l.SwName = name
	}

	return l, nil
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
