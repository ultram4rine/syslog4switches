package parsers

import (
	"errors"
	"strings"
	"time"

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
				facility, ok := val.(int)
				if !ok {
					return l, errors.New("facility wrong type")
				}
				l.Facility = uint8(facility)
			}
		case "severity":
			{
				severity, ok := val.(int)
				if !ok {
					return l, errors.New("severity wrong type")
				}
				l.Severity = uint8(severity)
			}
		case "priority":
			{
				priority, ok := val.(int)
				if !ok {
					return l, errors.New("priority wrong type")
				}
				l.Priority = uint8(priority)
			}
		}
	}

	return l, nil
}

func ParseSwitchLog(logmap format.LogParts) (switchLog, error) {
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
					return l, errors.New("content wrong type")
				}
			}
		case "client":
			{
				ip, ok := val.(string)
				if !ok {
					return l, errors.New("client wrong type")
				}
				l.IP = strings.Split(ip, ":")[0]
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
				facility, ok := val.(int)
				if !ok {
					return l, errors.New("facility wrong type")
				}
				l.Facility = uint8(facility)
			}
		case "severity":
			{
				severity, ok := val.(int)
				if !ok {
					return l, errors.New("severity wrong type")
				}
				l.Severity = uint8(severity)
			}
		case "priority":
			{
				priority, ok := val.(int)
				if !ok {
					return l, errors.New("priority wrong type")
				}
				l.Priority = uint8(priority)
			}
		}
	}

	return l, nil
}
