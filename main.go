package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	syslog "gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

var config struct {
	DBHost     string `json:"DBHost"`
	DBName     string `json:"DBName"`
	Collection string `json:"Collection"`
	DBUser     string `json:"DBUser"`
	DBPassword string `json:"DBPassword"`
}

type switchLog struct {
	SwName       string
	SwIP         string
	LogTimeStamp time.Time
	LogFacility  int
	LogSeverity  int
	LogPriority  int
	LogTime      string
	LogMessage   string
}

func main() {
	var (
		confPath = "conf.json"
		err      error
	)

	confdata, err := ioutil.ReadFile(confPath)
	if err != nil {
		log.Printf("Error reading config file: %s", err)
	}

	err = json.Unmarshal(confdata, &config)
	if err != nil {
		log.Printf("Error unmarshalling config file: %s", err)
	}

	opts := options.Client().ApplyURI(config.DBHost)
	opts = &options.ClientOptions{
		Auth: &options.Credential{
			Username: config.DBUser,
			Password: config.DBPassword,
		},
	}

	client, err := mongo.NewClient(opts)

	err = client.Connect(context.TODO())
	if err != nil {
		log.Printf("Error connecting to database: %s", err)
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Printf("Error checking connection: %s", err)
	}

	logsCollection := client.Database(config.DBName).Collection(config.Collection)

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

			_, err := logsCollection.InsertOne(context.TODO(), l)
			if err != nil {
				log.Printf("Error inserting log data to database: %s", err)
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
