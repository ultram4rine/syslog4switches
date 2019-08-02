package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	syslog "gopkg.in/mcuadros/go-syslog.v2"
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
	LogFacility  int
	LogSeverity  int
	LogPriority  int
	LogTime      string
	LogEventNum  string
	LogModule    string
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

	channel := make(syslog.LogPartsChannel)
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
			l := parseLog(logmap)

			tx, err := conn.Begin()
			if err != nil {
				log.Printf("Error starting transaction: %s", err)
			}

			loc, err := time.LoadLocation("Europe/Saratov")
			if err != nil {
				log.Printf("Error getting time zone: %s", err)
			}

			_, err = tx.Exec("INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_time, log_event_number, log_module, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", time.Now().In(loc), l.SwName, l.SwIP, l.LogTimeStamp, l.LogFacility, l.LogSeverity, l.LogPriority, l.LogTime, l.LogEventNum, l.LogModule, l.LogMessage)
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

func parseLog(logmap format.LogParts) switchLog {
	var l switchLog

	for key, val := range logmap {
		switch key {
		case "content":
			{
				valStr := val.(string)
				data := strings.Split(strings.Split(valStr, ": ")[0], " ")

				//TODO: Fix parsing date if day >= 10
				for i, d := range data {
					if i < 4 {
						l.LogTime += d + " "
					} else {
						switch i {
						case 4:
							l.SwName = d
						case 5:
							l.LogEventNum = d
						case 6:
							l.LogModule = d
						}
					}
				}

				l.LogMessage = strings.Split(valStr, ": ")[1]
			}
		case "client":
			l.SwIP = strings.Split(val.(string), ":")[0]
		case "timestamp":
			l.LogTimeStamp = val.(time.Time)
		case "facility":
			l.LogFacility = val.(int)
		case "severity":
			l.LogSeverity = val.(int)
		case "priority":
			l.LogPriority = val.(int)
		}
	}

	return l
}
