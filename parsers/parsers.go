package parsers

import (
	"errors"
	"strings"
	"time"

	"git.sgu.ru/ultramarine/syslog4switches/helpers"

	"gopkg.in/mcuadros/go-syslog.v2/format"
)

type switchLog struct {
	IP        string
	TimeStamp time.Time
	Facility  uint8
	Severity  uint8
	Priority  uint8
	Message   string
}

type nginxLog struct {
	Hostname  string
	TimeStamp time.Time
	Facility  uint8
	Severity  uint8
	Priority  uint8
	Message   string
}

type mailLog struct {
	Service   string
	TimeStamp time.Time
	Message   string
}

func ParseMailLog(logmap format.LogParts) (mailLog, error) {
	var (
		l  mailLog
		ok bool
	)

	for k, v := range logmap {
		switch k {
		case "tag":
			{
				l.Service, ok = v.(string)
				if !ok {
					return l, errors.New("tag wrong type")
				}
			}
		case "timestamp":
			{
				l.TimeStamp, ok = v.(time.Time)
				if !ok {
					return l, errors.New("timestamp wrong type")
				}
			}
		case "content":
			{
				l.Message, ok = v.(string)
				if !ok {
					return l, errors.New("content wrong type")
				}
			}
		}
	}

	return l, nil
}

func ParseNginxLog(logmap format.LogParts) (nginxLog, error) {
	var (
		l  nginxLog
		ok bool
	)

	for key, val := range logmap {
		switch key {
		case "content":
			{
				l.Message, ok = val.(string)
				if !ok {
					return l, errors.New("content wrong type")
				}
			}
		case "hostname":
			{
				l.Hostname, ok = val.(string)
				if !ok {
					return l, errors.New("hostname wrong type")
				}
			}
		case "timestamp":
			{
				l.TimeStamp, ok = val.(time.Time)
				if !ok {
					return l, errors.New("timestamp wrong type")
				}
			}
		case "facility":
			{
				l.Facility, ok = val.(uint8)
				if !ok {
					return l, errors.New("facility wrong type")
				}
			}
		case "severity":
			{
				l.Severity, ok = val.(uint8)
				if !ok {
					return l, errors.New("severity wrong type")
				}
			}
		case "priority":
			{
				l.Priority, ok = val.(uint8)
				if !ok {
					return l, errors.New("priority wrong type")
				}
			}
		}
	}

	return l, nil
}

func ParseSwitchLog(logmap format.LogParts, IPNameMap map[string]string) (string, switchLog, error) {
	var (
		l  switchLog
		ok bool
	)

	for key, val := range logmap {
		switch key {
		case "content":
			{
				l.Message, ok = val.(string)
				if !ok {
					return "", l, errors.New("content wrong type")
				}
			}
		case "client":
			{
				ip, ok := val.(string)
				if !ok {
					return "", l, errors.New("client wrong type")
				}
				l.IP = strings.Split(ip, ":")[0]
			}
		case "timestamp":
			{
				l.TimeStamp, ok = val.(time.Time)
				if !ok {
					return "", l, errors.New("timestamp wrong type")
				}
			}
		case "facility":
			{
				l.Facility, ok = val.(uint8)
				if !ok {
					return "", l, errors.New("facility wrong type")
				}
			}
		case "severity":
			{
				l.Severity, ok = val.(uint8)
				if !ok {
					return "", l, errors.New("severity wrong type")
				}
			}
		case "priority":
			{
				l.Priority, ok = val.(uint8)
				if !ok {
					return "", l, errors.New("priority wrong type")
				}
			}
		}
	}

	name, ok := IPNameMap[l.IP]
	if !ok {
		var err error
		name, err = helpers.GetSwitchName(l.IP)
		if err != nil {
			return "", switchLog{}, err
		}
	}

	return name, l, nil
}
