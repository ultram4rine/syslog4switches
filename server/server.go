package server

import (
	"context"
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

	ctx  context.Context
	DB   *sqlx.DB
	Tx   *sqlx.Tx
	Rows int64

	IPNameMap map[string]string
	Loc       *time.Location

	//NginxStmt  *sqlx.Stmt
	//MailStmt   *sqlx.Stmt
	//SwitchStmt *sqlx.Stmt
}

func (s *Server) Init(confname *string) error {
	if err := conf.Load(*confname); err != nil {
		return fmt.Errorf("failed to load configuration: %v", err)
	}

	s.ctx = context.Background()
	var err error

	s.DB, err = sqlx.ConnectContext(s.ctx, "clickhouse", fmt.Sprintf("%s?username=%s&password=%s&database=%s", conf.Config.DBHost, conf.Config.DBUser, conf.Config.DBPass, conf.Config.DBName))
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	defer s.DB.Close()

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

	if err := s.InitSQL(); err != nil {
		return err
	}

	return nil
}

func (s *Server) InitSQL() (err error) {
	s.Tx, err = s.DB.BeginTxx(s.ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	/*s.NginxStmt, err = s.Tx.PreparexContext(s.ctx, nginxQuery)
	if err != nil {
		if err := s.Tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error creating nginx statement: %v", err)
	}
	s.MailStmt, err = s.Tx.PreparexContext(s.ctx, mailQuery)
	if err != nil {
		if err := s.Tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error creating mail statement: %v", err)
	}
	s.SwitchStmt, err = s.Tx.PreparexContext(s.ctx, switchQuery)
	if err != nil {
		if err := s.Tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error creating switch statement: %v", err)
	}*/

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

func (s *Server) Flush() error {
	if s.Rows == 0 {
		return nil
	}

	/*s.NginxStmt.Close()
	s.NginxStmt = nil
	s.MailStmt.Close()
	s.MailStmt = nil
	s.SwitchStmt.Close()
	s.SwitchStmt = nil*/

	err := s.Tx.Commit()
	s.Rows = 0
	s.Tx = nil

	return err
}

func (s *Server) saveNginxLog(logmap format.LogParts) error {
	l, err := parsers.ParseNginxLog(logmap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	if _, err := s.Tx.ExecContext(s.ctx, nginxQuery, l.Hostname, l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
		if err := s.Tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	} else {
		if err = s.Tx.Commit(); err != nil {
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

	// Substract 4 hours because parsing time from rsyslog don't sets current timezone.
	if _, err := s.Tx.ExecContext(s.ctx, mailQuery, l.Service, l.TimeStamp.In(s.Loc).Add(-4*time.Hour), l.Message); err != nil {
		if err := s.Tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	} else {
		if err = s.Tx.Commit(); err != nil {
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

	if _, err := s.Tx.ExecContext(s.ctx, switchQuery, time.Now().In(s.Loc), name, net.ParseIP(l.IP), l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
		if err := s.Tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error inserting log to database: %v", err)
	} else {
		if err = s.Tx.Commit(); err != nil {
			return fmt.Errorf("error commiting transaction: %v", err)
		}
	}

	return nil
}
