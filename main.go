package main

import (
	"fmt"
	"log"

	syslog "gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mgo.v2/bson"
)

var config struct {
	DBHost string `json:"DBHost"`
}

type switchLog struct {
	ID        bson.ObjectId `bson:"_id"`
	SwName    string        `bson:"swname"`
	SwIP      string        `bson:"swip"`
	LogString string        `bson:"logstring"`
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
	server.SetFormat(syslog.RFC5424)
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
		for log := range channel {
			for k, v := range log {
				fmt.Println(k, ": ", v)
			}
		}
	}(channel)

	server.Wait()
}
