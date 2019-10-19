package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	snmp "github.com/soniah/gosnmp"
	"gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

var config struct {
	DBHost     string `json:"dbHost"`
	DBName     string `json:"dbName"`
	DBUser     string `json:"dbUser"`
	DBPassword string `json:"dbPassword"`
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

func main() {
	var (
		confPath = "conf.json"
		err      error
	)

	confdata, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Fatalf("Error reading config file: %s", err)
	}

	err = json.Unmarshal(confdata, &config)
	if err != nil {
		log.Fatalf("Error unmarshalling config file: %s", err)
	}

	conn, err := sqlx.Open("clickhouse", config.DBHost+"?username="+config.DBUser+"&password="+config.DBPassword+"&database="+config.DBName)
	if err != nil {
		log.Fatalf("Error connection to database: %s", err)
	}
	defer conn.Close()

	loc, err := time.LoadLocation("Europe/Saratov")
	if err != nil {
		log.Fatalf("Error getting time zone: %s", err)
	}

	channel := make(syslog.LogPartsChannel, 1000)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	server.SetFormat(syslog.Automatic)
	server.SetHandler(handler)

	err = server.ListenUDP(":514")
	if err != nil {
		log.Printf("Error configuring server for UDP listen: %s", err)
	}

	err = server.Boot()
	if err != nil {
		log.Printf("Error starting server: %s", err)
	}

	go func(channel syslog.LogPartsChannel) {
		for logmap := range channel {
			l, err := parseLog(logmap)
			if err != nil {
				log.Printf("Error parsing log: %s", err)
				continue
			}

			tx, err := conn.Begin()
			if err != nil {
				log.Printf("Error starting transaction: %s", err)
				continue
			}

			_, err = tx.Exec("INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?)", time.Now().In(loc), l.SwName, net.ParseIP(l.SwIP), l.LogTimeStamp, l.LogFacility, l.LogSeverity, l.LogPriority, l.LogMessage)
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
	}(channel)

	server.Wait()
}

func parseLog(logmap format.LogParts) (switchLog, error) {
	var (
		l   switchLog
		err error
	)

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

	l.SwName, err = getSwitchName(l.SwIP)
	if err != nil {
		return l, err
	}

	return l, nil
}

func getSwitchName(IP string) (string, error) {
	var switchName string

	snmp.Default.Target = IP

	err := snmp.Default.Connect()
	if err != nil {
		return "", err
	}
	defer snmp.Default.Conn.Close()

	oid := []string{".1.3.6.1.2.1.1.5.0"}

	result, err := snmp.Default.Get(oid)
	if err != nil {
		return "", err
	}

	for _, variable := range result.Variables {
		if variable.Type != snmp.OctetString {
			return "", errors.New("can't get switch name")
		}

		bytes := variable.Value.([]byte)
		switchName = string(bytes)
	}

	return switchName, nil
}
