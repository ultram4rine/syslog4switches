package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"git.sgu.ru/ultramarine/custom_syslog/conf"
	"git.sgu.ru/ultramarine/custom_syslog/helpers"
	"git.sgu.ru/ultramarine/custom_syslog/savers"
	"google.golang.org/grpc"

	pb "git.sgu.ru/sgu/netdataserv/netdataproto"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/mcuadros/go-syslog.v2"
)

var confname = kingpin.Flag("conf", "Path to config file.").Short('c').Default("csyslog.conf").String()

func main() {
	kingpin.Parse()

	if err := conf.Load(*confname); err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	var ctx = context.Background()

	db, err := sqlx.ConnectContext(ctx, "clickhouse", fmt.Sprintf("%s?username=%s&password=%s&database=%s", conf.Config.DBHost, conf.Config.DBUser, conf.Config.DBPass, conf.Config.DBName))
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer db.Close()

	conn, err := grpc.Dial(conf.Config.NetDataServer, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error connecting to netdata server: %v", err)
	}
	defer conn.Close()
	log.Info("connected to netdata server")

	client := pb.NewNetDataClient(conn)
	IPNameMap, err := helpers.GetSwitches(client)
	if err != nil {
		log.Warnf("error getting switches from netdataserv: %v", err)
	}

	channel := make(syslog.LogPartsChannel, 1000)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	server.SetFormat(syslog.Automatic)
	server.SetHandler(handler)

	if err = server.ListenUDP(":514"); err != nil {
		log.Fatalf("Error configuring server for UDP listen: %v", err)
	}

	if err = server.Boot(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}

	loc, err := time.LoadLocation("Europe/Saratov")
	if err != nil {
		log.Fatalf("Error getting time zone: %v", err)
	}

	go func(channel syslog.LogPartsChannel) {
		for logmap := range channel {
			log.Infof("Received log from %v", logmap["client"])

			tag, ok := logmap["tag"].(string)
			if !ok {
				log.Warn("tag wrong type")
				continue
			}

			switch {
			case tag == "nginx":
				savers.SaveNginxLog(ctx, db, logmap)
			case strings.Contains(tag, "postfix") || strings.Contains(tag, "dovecot"):
				savers.SaveMailLog(ctx, db, logmap)
			case tag == "":
				savers.SaveSwitchLog(ctx, db, logmap, loc, IPNameMap)
			}
		}
	}(channel)

	server.Wait()
}
