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

	NConn *sqlx.DB
	MConn *sqlx.DB
	SConn *sqlx.DB

	NTx *sqlx.Tx
	MTx *sqlx.Tx
	STx *sqlx.Tx

	NStmt *sqlx.Stmt
	MStmt *sqlx.Stmt
	SStmt *sqlx.Stmt

	NRows int64
	MRows int64
	SRows int64

	IPNameMap map[string]string
	Loc       *time.Location
}

func (s *Server) Init(confname *string) error {
	var err error

	if err := conf.Load(*confname); err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	s.NConn, err = sqlx.Connect("clickhouse", fmt.Sprintf("%s?username=%s&password=%s&database=%s", conf.Config.DBHost, conf.Config.DBUser, conf.Config.DBPass, conf.Config.DBName))
	if err != nil {
		return fmt.Errorf("failed to create connection for nginx logs: %v", err)
	}

	s.MConn, err = sqlx.Connect("clickhouse", fmt.Sprintf("%s?username=%s&password=%s&database=%s", conf.Config.DBHost, conf.Config.DBUser, conf.Config.DBPass, conf.Config.DBName))
	if err != nil {
		return fmt.Errorf("failed to create connection for mail logs: %v", err)
	}

	s.SConn, err = sqlx.Connect("clickhouse", fmt.Sprintf("%s?username=%s&password=%s&database=%s", conf.Config.DBHost, conf.Config.DBUser, conf.Config.DBPass, conf.Config.DBName))
	if err != nil {
		return fmt.Errorf("failed to create connection for switch logs: %v", err)
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

	if err := s.InitNginx(); err != nil {
		return err
	}
	if err := s.InitMail(); err != nil {
		return err
	}
	if err := s.InitSwitch(); err != nil {
		return err
	}

	return nil
}

func (s *Server) InitNginx() (err error) {
	s.NTx, err = s.NConn.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for nginx logs: %v", err)
	}

	s.NStmt, err = s.NTx.Preparex(nginxQuery)
	if err != nil {
		if err := s.NTx.Rollback(); err != nil {
			return fmt.Errorf("error aborting nginx transaction: %v", err)
		}
		return fmt.Errorf("error creating nginx statement: %v", err)
	}

	return nil
}

func (s *Server) InitMail() (err error) {
	s.MTx, err = s.MConn.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for mail logs: %v", err)
	}

	s.MStmt, err = s.MTx.Preparex(mailQuery)
	if err != nil {
		if err := s.MTx.Rollback(); err != nil {
			return fmt.Errorf("error aborting mail transaction: %v", err)
		}
		return fmt.Errorf("error creating mail statement: %v", err)
	}

	return nil
}

func (s *Server) InitSwitch() (err error) {
	s.STx, err = s.SConn.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for switch logs: %v", err)
	}

	s.SStmt, err = s.STx.Preparex(switchQuery)
	if err != nil {
		if err := s.STx.Rollback(); err != nil {
			return fmt.Errorf("error aborting switch transaction: %v", err)
		}
		return fmt.Errorf("error creating switch statement: %v", err)
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

func (s *Server) FlushNginx() error {
	if s.NRows == 0 {
		return nil
	}

	s.NStmt.Close()
	s.NStmt = nil

	err := s.NTx.Commit()
	s.NRows = 0
	s.NTx = nil

	return err
}

func (s *Server) FlushMail() error {
	if s.MRows == 0 {
		return nil
	}

	s.MStmt.Close()
	s.MStmt = nil

	err := s.MTx.Commit()
	s.MRows = 0
	s.MTx = nil

	return err
}

func (s *Server) FlushSwitch() error {
	if s.SRows == 0 {
		return nil
	}

	s.SStmt.Close()
	s.SStmt = nil

	err := s.STx.Commit()
	s.SRows = 0
	s.STx = nil

	return err
}

func (s *Server) saveNginxLog(logmap format.LogParts) error {
	if s.NStmt == nil {
		if err := s.InitNginx(); err != nil {
			return fmt.Errorf("error initializing nginx tx and stmt: %v", err)
		}
	}

	l, err := parsers.ParseNginxLog(logmap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	if _, err := s.NStmt.Exec(l.Hostname, l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
		if err := s.NTx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	}

	s.NRows++
	if s.NRows >= conf.Config.NginxBatchSize {
		if err := s.FlushNginx(); err != nil {
			return fmt.Errorf("error commiting nginx tx: %v", err)
		}
	}

	return nil
}

func (s *Server) saveMailLog(logmap format.LogParts) error {
	if s.MStmt == nil {
		if err := s.InitMail(); err != nil {
			return fmt.Errorf("error initializing mail tx and stmt: %v", err)
		}
	}

	l, err := parsers.ParseMailLog(logmap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	if l.Service == "dovecot" && !(strings.Contains(l.Message, "expunged")) {
		return nil
	}

	// Substract 4 hours because parsing time from rsyslog don't sets current timezone.
	if _, err := s.MStmt.Exec(l.Service, l.TimeStamp.In(s.Loc).Add(-4*time.Hour), l.Message); err != nil {
		if err := s.MTx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	}

	s.MRows++
	if s.MRows >= conf.Config.MailBatchSize {
		if err := s.FlushMail(); err != nil {
			return fmt.Errorf("error commiting mail tx: %v", err)
		}
	}

	return nil
}

func (s *Server) saveSwitchLog(logmap format.LogParts) error {
	if s.SStmt == nil {
		if err := s.InitSwitch(); err != nil {
			return fmt.Errorf("error initializing switch tx and stmt: %v", err)
		}
	}

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

	if _, err := s.SStmt.Exec(time.Now().In(s.Loc), name, net.ParseIP(l.IP), l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
		if err := s.STx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	}

	s.SRows++
	if s.SRows >= conf.Config.SwitchBatchSize {
		if err := s.FlushSwitch(); err != nil {
			return fmt.Errorf("error commiting switch tx: %v", err)
		}
	}

	return nil
}
