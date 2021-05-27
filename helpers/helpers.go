package helpers

import (
	"context"
	"errors"

	pb "git.sgu.ru/sgu/netdataserv/netdataproto"
	"github.com/gosnmp/gosnmp"
)

func GetSwitches(c pb.NetDataClient) (map[string]string, error) {
	var ctx = context.Background()

	switches, err := c.GetNetworkSwitches(ctx, &pb.GetNetworkSwitchesRequest{})
	if err != nil {
		return nil, err
	}

	var IPNameMap = make(map[string]string)
	for _, s := range switches.Switch {
		IPNameMap[s.Ipv4Address] = s.Name
	}

	return IPNameMap, nil
}

func GetSwitchNameSNMP(ip string) (name string, err error) {
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
			return "", errors.New("wrong OID")
		}
	}

	return name, nil
}
