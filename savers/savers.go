package savers

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"git.sgu.ru/ultramarine/custom_syslog/helpers"
	"git.sgu.ru/ultramarine/custom_syslog/parsers"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

const (
	switchQuery = "INSERT INTO switchlogs (ts_local, sw_name, sw_ip, ts_remote, facility, severity, priority, log_msg) VALUES (?, ?, ?, ?, ?, ?, ?)"
	nginxQuery  = "INSERT INTO nginx (hostname, timestamp, facility, severity, priority, message) VALUES (?, ?, ?, ?, ?, ?)"
	mailQuery   = "INSERT INTO mail (service, timestamp, message) VALUES (?, ?, ?)"
)

func SaveNginxLog(ctx context.Context, db *sqlx.DB, logmap format.LogParts) error {
	l, err := parsers.ParseNginxLog(logmap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}

	stmt, err := tx.PreparexContext(ctx, nginxQuery)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error creating insert statement: %v", err)
	}
	defer stmt.Close()

	if _, err := stmt.ExecContext(ctx, l.Hostname, l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
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

func SaveMailLog(ctx context.Context, db *sqlx.DB, logmap format.LogParts, loc *time.Location) error {
	l, err := parsers.ParseMailLog(logmap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	if l.Service == "dovecot" && !(strings.Contains(l.Message, "expunged")) {
		return nil
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}

	stmt, err := tx.PreparexContext(ctx, mailQuery)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error creating insert statement: %v", err)
	}
	defer stmt.Close()

	// Substract 4 hours because parsing time from rsyslog don't sets current timezone.
	if _, err := stmt.ExecContext(ctx, l.Service, l.TimeStamp.In(loc).Add(-4*time.Hour), l.Message); err != nil {
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

func SaveSwitchLog(ctx context.Context, db *sqlx.DB, logmap format.LogParts, loc *time.Location, IPNameMap map[string]string) error {
	l, err := parsers.ParseSwitchLog(logmap, IPNameMap)
	if err != nil {
		return fmt.Errorf("error parsing log: %v", err)
	}

	name, ok := IPNameMap[l.IP]
	if !ok {
		log.Infof("switch: unknown IP %s, going to find name via SNMP", l.IP)
		name, err = helpers.GetSwitchNameSNMP(l.IP)
		if err != nil {
			return fmt.Errorf("error getting switch name by SNMP: %v", err)
		}
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}

	stmt, err := tx.PreparexContext(ctx, switchQuery)
	if err != nil {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("error aborting transaction: %v", err)
		}
		return fmt.Errorf("error creating insert statement: %v", err)
	}
	defer stmt.Close()

	if _, err := stmt.ExecContext(ctx, time.Now().In(loc), name, net.ParseIP(l.IP), l.TimeStamp, l.Facility, l.Severity, l.Priority, l.Message); err != nil {
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
