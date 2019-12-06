package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	"gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

var config struct {
	DBHost     string `json:"dbHost"`
	DBName     string `json:"dbName"`
	DBUser     string `json:"dbUser"`
	DBPassword string `json:"dbPassword"`

	DBNetmapHost string `json:"dbNetmapHost"`
	DBNetmapName string `json:"dbNetmapName"`
	DBNetmapUser string `json:"dbNetmapUser"`
	DBNetmapPass string `json:"dbNetmapPass"`

	IPNet string `json:"ipNet"`
}

type netmapSwitch struct {
	Name string `db:"name"`
	IP   string `db:"ip"`
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
		confPath = "syslog4switches.conf.json"
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

	_, network, err := net.ParseCIDR(config.IPNet)
	if err != nil {
		log.Fatalf("Error parsing network's IP %s: %s", config.IPNet, err)
	}

	dbConf := mysql.NewConfig()

	dbConf.Net = "tcp"
	dbConf.Addr = config.DBNetmapHost
	dbConf.DBName = config.DBNetmapName
	dbConf.User = config.DBNetmapUser
	dbConf.Passwd = config.DBNetmapPass
	dbConf.ParseTime = true

	dbNetmap, err := sqlx.Open("mysql", dbConf.FormatDSN())
	if err != nil {
		log.Fatalf("Error connecting to netmap database: %s", err)
	}
	defer dbNetmap.Close()

	conn, err := sqlx.Open("clickhouse", config.DBHost+"?username="+config.DBUser+"&password="+config.DBPassword+"&database="+config.DBName)
	if err != nil {
		log.Fatalf("Error connecting to database: %s", err)
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

	err = server.ListenUnixgram(":514")
	if err != nil {
		log.Fatalf("Error configuring server for UDP listen: %s", err)
	}

	err = server.Boot()
	if err != nil {
		log.Fatalf("Error starting server: %s", err)
	}

	swMap := make(map[string]string)

	go func(db *sqlx.DB) {
		swMap, err = makeSwitchMap(db)
		if err != nil {
			log.Printf("Error making map[ip]name: %s", err)
		}
		log.Printf("Map of ip's and names maked!")

		for range time.Tick(time.Minute * 30) {
			swMap, err = makeSwitchMap(db)
			if err != nil {
				log.Printf("Error making map[ip]name: %s", err)
			}
			log.Printf("Map of ip's and names updated!")
		}
	}(dbNetmap)

	go func(channel syslog.LogPartsChannel) {
		for logmap := range channel {
			if l, err := parseLog(logmap, network, swMap); err == nil {
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
		}
	}(channel)

	server.Wait()
}

func parseLog(logmap format.LogParts, network *net.IPNet, swMap map[string]string) (switchLog, error) {
	var (
		l switchLog
	)

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

	l.SwName = swMap[l.SwIP]

	return l, nil
}

func makeSwitchMap(db *sqlx.DB) (map[string]string, error) {
	var (
		swMap    = make(map[string]string)
		switches []netmapSwitch
	)

	err := db.Select(&switches, "SELECT name, ip FROM unetmap_host WHERE ip IS NOT NULL AND type_id = 4")
	if err != nil {
		return nil, err
	}

	for _, sw := range switches {
		intIP, err := strconv.Atoi(sw.IP)
		if err != nil {
			log.Printf("Error converting string IP to int IP: %s", err)
			return swMap, err
		}

		realIP := fmt.Sprintf("%d.%d.%d.%d", byte(intIP>>24), byte(intIP>>16), byte(intIP>>8), byte(intIP))

		swMap[realIP] = sw.Name
	}

	return swMap, nil
}
