package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	syslog "gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
	"gopkg.in/mgo.v2/bson"
)

var config struct {
	DBHost string `json:"DBHost"`
}

type switchLog struct {
	ID           bson.ObjectId `bson:"_id"`
	SwName       string        `bson:"swname"`
	SwIP         string        `bson:"swip"`
	LogTime      string        `bson:"logtime"`
	LogMessage   string        `bson:"logmsg"`
	LogTimeStamp time.Time     `bson:"logtimestamp"`
	LogFacility  int           `bson:"logfac"`
	LogSeverity  int           `bson:"logsev"`
	LogPriority  int           `bson:"logpri"`
}

func main() {
	var (
		//confPath = "conf.json"
		err error
	)

	/*confdata, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Printf("Error reading config file: %s", err)
	}

	err = json.Unmarshal(confdata, &config)
	if err != nil {
		log.Printf("Error unmarshalling config file: %s", err)
	}

	session, err := mgo.Dial(config.DBHost)
	if err != nil {
		log.Printf("Error connectiong to database: %s", err)
	}
	defer session.Close()

	logsCollection := session.DB("logsdb").C("logs")*/

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
			fmt.Println("Facility:", l.LogFacility)
			fmt.Println("Severity:", l.LogSeverity)
			fmt.Println("Priority:", l.LogPriority)
			fmt.Println("Switch Name:", l.SwName)
			fmt.Println("Switch IP:", l.SwIP)
			fmt.Println("Time:", l.LogTime)
			fmt.Println("TimeStamp:", l.LogTimeStamp)
			fmt.Println("Message:", l.LogMessage)
			fmt.Println()
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

				for i, d := range data {
					if i < 3 {
						l.LogTime += d + " "
					} else {
						switch i {
						case 3:
							l.SwName = d
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
