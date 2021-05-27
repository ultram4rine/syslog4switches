package savers

import (
	"context"
	"net"
	"strings"
	"time"

	"git.sgu.ru/ultramarine/syslog4switches/helpers"
	"git.sgu.ru/ultramarine/syslog4switches/parsers"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

const (
	switchQuery = "INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?)"
	nginxQuery  = "INSERT INTO nginx (hostname, timestamp, facility, severity, priority, message) VALUES (?, ?, ?, ?, ?, ?)"
	mailQuery   = "INSERT INTO mail (service, timestamp, message) VALUES (?, ?, ?)"
)

func SaveNginxLog(ctx context.Context, db *sqlx.DB, logmap format.LogParts) {
	l, err := parsers.ParseNginxLog(logmap)
	if err != nil {
		log.Warnf("nginx: error parsing log: %v", err)
		return
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		log.Warnf("nginx: error starting transaction: %v", err)
		return
	}

	stmt, err := tx.PreparexContext(ctx, nginxQuery)
	if err != nil {
		log.Warnf("nginx: error creating insert statement: %v", err)
		if err := tx.Rollback(); err != nil {
			log.Warnf("nginx: error aborting transaction: %v", err)
		}
		return
	}
	defer stmt.Close()

	if _, err := stmt.ExecContext(ctx, l.Hostname, l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
		log.Warnf("nginx: error inserting log to database: %v", err)
		if err := tx.Rollback(); err != nil {
			log.Warnf("nginx: error aborting transaction: %v", err)
			return
		}
	} else {
		if err = tx.Commit(); err != nil {
			log.Warnf("nginx: error commiting transaction: %v", err)
			return
		}
	}
}

func SaveMailLog(ctx context.Context, db *sqlx.DB, logmap format.LogParts) {
	l, err := parsers.ParseMailLog(logmap)
	if err != nil {
		log.Warnf("mail: error parsing log: %v", err)
		return
	}

	if l.Service == "dovecot" && !(strings.Contains(l.Message, "expunged")) {
		return
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		log.Warnf("mail: error starting transaction: %v", err)
		return
	}

	stmt, err := tx.PreparexContext(ctx, mailQuery)
	if err != nil {
		log.Warnf("mail: error creating insert statement: %v", err)
		if err := tx.Rollback(); err != nil {
			log.Warnf("mail: error aborting transaction: %v", err)
		}
		return
	}
	defer stmt.Close()

	if _, err := stmt.ExecContext(ctx, l.Service, l.TimeStamp, l.Message); err != nil {
		log.Warnf("mail: error inserting log to database: %v", err)
		if err := tx.Rollback(); err != nil {
			log.Warnf("mail: error aborting transaction: %v", err)
			return
		}
	} else {
		if err = tx.Commit(); err != nil {
			log.Warnf("mail: error commiting transaction: %v", err)
			return
		}
	}
}

func SaveSwitchLog(ctx context.Context, db *sqlx.DB, logmap format.LogParts, loc *time.Location, IPNameMap map[string]string) {
	l, err := parsers.ParseSwitchLog(logmap, IPNameMap)
	if err != nil {
		log.Warnf("switch: error parsing log: %v", err)
		return
	}

	name, ok := IPNameMap[l.IP]
	if !ok {
		log.Infof("switch: unknown IP %s, going to find name via SNMP", l.IP)
		name, err = helpers.GetSwitchNameSNMP(l.IP)
		if err != nil {
			log.Warnf("switch: error getting switch name by SNMP: %v", err)
			return
		}
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		log.Warnf("switch: error starting transaction: %v", err)
		return
	}

	stmt, err := tx.PreparexContext(ctx, switchQuery)
	if err != nil {
		log.Warnf("switch: error creating insert statement: %v", err)
		if err := tx.Rollback(); err != nil {
			log.Warnf("switch: error aborting transaction: %v", err)
		}
		return
	}
	defer stmt.Close()

	if _, err := stmt.ExecContext(ctx, time.Now().In(loc), name, net.ParseIP(l.IP), l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
		log.Warnf("switch: error inserting log to database: %v", err)
		if err := tx.Rollback(); err != nil {
			log.Warnf("switch: error aborting transaction: %v", err)
			return
		}
	} else {
		if err = tx.Commit(); err != nil {
			log.Warnf("switch: error commiting transaction: %v", err)
			return
		}
	}
}
