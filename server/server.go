package server

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"git.sgu.ru/ultramarine/custom_syslog/conf"
	"git.sgu.ru/ultramarine/custom_syslog/helpers"
	"git.sgu.ru/ultramarine/custom_syslog/parsers"

	pb "git.sgu.ru/sgu/netdataserv/netdataproto"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

const (
	switchQuery = "INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?)"
	nginxQuery  = "INSERT INTO nginx (hostname, timestamp, facility, severity, priority, message) VALUES (?, ?, ?, ?, ?, ?)"
	mailQuery   = "INSERT INTO mail (service, timestamp, message) VALUES (?, ?, ?)"
)

type Server struct {
	SyslogServer *syslog.Server

	Conn *sqlx.DB

	IPNameMap map[string]string
	Loc       *time.Location
}

func (s *Server) Init(confname *string) error {
	var err error

	if err := conf.Load(*confname); err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	var dataSource = fmt.Sprintf("%s?username=%s&password=%s&database=%s", conf.Config.DBHost, conf.Config.DBUser, conf.Config.DBPass, conf.Config.DBName)
	s.Conn, err = sqlx.Connect("clickhouse", dataSource)
	if err != nil {
		return fmt.Errorf("failed to create connection for nginx logs: %v", err)
	}
	if err := s.Conn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return fmt.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			return err
		}
	}

	conn, err := grpc.Dial(conf.Config.NetDataServer, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to netdata server: %v", err)
	}
	defer conn.Close()

	client := pb.NewNetDataClient(conn)
	s.IPNameMap, err = helpers.GetSwitches(client)
	if err != nil {
		log.Warnf("error getting switches from netdataserv: %v", err)
	}

	s.Loc, err = time.LoadLocation("Europe/Saratov")
	if err != nil {
		return fmt.Errorf("failed to get location: %v", err)
	}

	return nil
}

func (s *Server) ProcessLog(logmap format.LogParts) error {
	tag, ok := logmap["tag"].(string)
	if !ok {
		return errors.New("tag wrong type")
	}

	switch {
	case tag == "nginx":
		if err := s.saveNginxLog(logmap); err != nil {
			return fmt.Errorf("nginx: %v", err)
		}
	case strings.Contains(tag, "postfix") || strings.Contains(tag, "dovecot"):
		if err := s.saveMailLog(logmap); err != nil {
			return fmt.Errorf("mail: %v", err)
		}
	case tag == "":
		if err := s.saveSwitchLog(logmap); err != nil {
			return fmt.Errorf("switch: %v", err)
		}
	}

	return nil
}

func (s *Server) saveNginxLog(logmap format.LogParts) error {
	l, err := parsers.ParseNginxLog(logmap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	tx, err := s.Conn.Beginx()
	if err != nil {
		return fmt.Errorf("error creating transaction: %v", err)
	}

	if _, err := tx.Exec(nginxQuery, l.Hostname, l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	} else {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("error commiting transaction: %v", err)
		}
	}

	return nil
}

func (s *Server) saveMailLog(logmap format.LogParts) error {
	l, err := parsers.ParseMailLog(logmap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	if l.Service == "dovecot" && !(strings.Contains(l.Message, "expunged")) {
		return nil
	}

	tx, err := s.Conn.Beginx()
	if err != nil {
		return fmt.Errorf("error creating transaction: %v", err)
	}

	// Substract 4 hours because parsing time from rsyslog don't sets current timezone.
	if _, err := tx.Exec(mailQuery, l.Service, l.TimeStamp.In(s.Loc).Add(-4*time.Hour), l.Message); err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	} else {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("error commiting transaction: %v", err)
		}
	}

	return nil
}

func (s *Server) saveSwitchLog(logmap format.LogParts) error {
	l, err := parsers.ParseSwitchLog(logmap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	name, ok := s.IPNameMap[l.IP]
	if !ok {
		log.Infof("switch: unknown IP %s, going to find name via SNMP", l.IP)
		name, err = helpers.GetSwitchNameSNMP(l.IP)
		if err != nil {
			return fmt.Errorf("error getting switch name by SNMP: %v", err)
		}
		s.IPNameMap[l.IP] = name
	}

	tx, err := s.Conn.Beginx()
	if err != nil {
		return fmt.Errorf("error creating transaction: %v", err)
	}

	if _, err := tx.Exec(switchQuery, time.Now().In(s.Loc), name, net.ParseIP(l.IP), l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	} else {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("error commiting transaction: %v", err)
		}
	}

	return nil
}
