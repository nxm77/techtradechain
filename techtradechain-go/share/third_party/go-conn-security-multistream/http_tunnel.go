package csms

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
)

var regId = regexp.MustCompile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")

const (
	// LIBP2P_HTTP_TUNNEL_TARGET_ADDRESS_LIST=gH/127.0.0.1:8000,45/127.0.0.1:8001,6H/example.org
	HttpTunnelTargetAddressList = "LIBP2P_HTTP_TUNNEL_TARGET_ADDRESS_LIST"
	// QmeyNRs2DwWjcHTpcVHoUSaDAAif4VQZ2wQDQAUNDP33gH,QmXf6mnQDBR9aHauRmViKzSuZgpumkn7x6rNxw1oqqRr45,QmRRWXJpAVdhFsFtd9ah5F4LDQWFFBDVKpECAF8hssqj6H
	// LIBP2P_HTTP_TUNNEL_WHITE_LIST=gH,45,6H
	HttpTunnelWhiteList = "LIBP2P_HTTP_TUNNEL_WHITE_LIST"
	// LIBP2P_HTTP_TUNNEL_DEFAULT_TARGET_ADDRESS=example.org
	HttpTunnelDefaultTargetAddress = "LIBP2P_HTTP_TUNNEL_DEFAULT_TARGET_ADDRESS"
)

// getSendHttpTunnelTargetAddress 如果id在白名单中，则按照顺序找targetAddress
func getSendHttpTunnelTargetAddress(pid string) string {
	// query node list
	addressList := os.Getenv(HttpTunnelTargetAddressList)
	addressListArr := strings.Split(addressList, ",")
	for _, v := range addressListArr {
		index := strings.Index(v, "/")
		if index > 0 && len(v) > index+1 && strings.HasSuffix(pid, v[:index]) {
			return formatAddress(v[index+1:])
		}
	}
	// query white list
	whiteList := os.Getenv(HttpTunnelWhiteList)
	nodes := strings.Split(whiteList, ",")
	for _, nodeId := range nodes {
		if strings.HasSuffix(pid, nodeId) {
			//use default address
			return formatAddress(os.Getenv(HttpTunnelDefaultTargetAddress))
		}
	}
	return ""
}

func formatAddress(src string) string {
	if regId != nil && regId.MatchString(src) {
		return "//" + src
	}
	return src
}

func sendHttpTunnelReq(conn net.Conn, targetAddress string) error {
	if conn == nil {
		return errors.New("conn == nil")
	}
	fmt.Println(fmt.Sprintf("sendHttpTunnelReq localAddr(%v)remoteAddr(%v)targetAddr(%v)", conn.LocalAddr(), conn.RemoteAddr(), targetAddress))
	req, err := http.NewRequest("CONNECT", targetAddress, nil)
	if err != nil {
		return fmt.Errorf("NewRequest err:%v", err)
	}
	err = req.Write(conn)
	if err != nil {
		return fmt.Errorf("write err:%v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return fmt.Errorf("ReadResponse err:%v", err)
	}
	if resp == nil {
		return fmt.Errorf("resp == nil")
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to establish HTTP tunnel code(%v)", resp.StatusCode)
	}
	return nil
}
