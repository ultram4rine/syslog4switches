package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
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
	LogFacility  uint8
	LogSeverity  uint8
	LogPriority  uint8
	LogTime      time.Time
	LogEventNum  uint16
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
			l, err := parseLog(logmap)
			if err != nil {
				log.Printf("Error parsing log: %s", err)
			}

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

func parseLog(logmap format.LogParts) (switchLog, error) {
	var l switchLog

	for key, val := range logmap {
		switch key {
		case "content":
			{
				var (
					err     error
					logTime string
					valStr  = val.(string)
					dataStr = strings.Split(valStr, ": ")[0]
				)

				reg := regexp.MustCompile(`[\s\p{Zs}]{2,}`)
				dataStr = reg.ReplaceAllString(dataStr, " ")

				data := strings.Split(dataStr, " ")

				for i, d := range data {
					if i < 3 {
						logTime += d + " "
					} else {
						switch i {
						case 3:
							l.SwName = d
						case 4:
							eventNum, err := strconv.ParseUint(d, 10, 16)
							if err != nil {
								return l, err
							} else {
								l.LogEventNum = uint16(eventNum)
							}
						case 5:
							l.LogModule = d
						}
					}
				}

				l.LogMessage = strings.Split(valStr, ": ")[1]
				l.LogTime, err = time.Parse("Jan 2 15:04:05", logTime)
				if err != nil {
					return l, err
				}
			}
		case "client":
			l.SwIP = strings.Split(val.(string), ":")[0]
		case "timestamp":
			l.LogTimeStamp = val.(time.Time)
		case "facility":
			l.LogFacility = val.(uint8)
		case "severity":
			l.LogSeverity = val.(uint8)
		case "priority":
			l.LogPriority = val.(uint8)
		}
	}

	return l, nil
}
