package helpers

import (
	"errors"

	"github.com/soniah/gosnmp"
)

func GetSwitchName(ip string) (name string, err error) {
	const sysName = ".1.3.6.1.2.1.1.5.0"

	sw := gosnmp.Default
	sw.Target = ip
	sw.Retries = 2

	if err := sw.Connect(); err != nil {
		return "", err
	}
	defer sw.Conn.Close()

	oids := []string{sysName}
	result, err := sw.Get(oids)
	if err != nil {
		return "", err
	}

	for _, v := range result.Variables {
		switch v.Name {
		case sysName:
			name = v.Value.(string)
		default:
			return "", errors.New("something went wrong :(")
		}
	}

	return name, nil
}
