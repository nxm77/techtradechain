/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
)

func sendHttpTunnelReq(conn net.Conn, targetAddress string) error {
	if conn == nil {
		return errors.New("conn == nil")
	}
	//fmt.Printf("sendHttpTunnelReq localAddr(%v)remoteAddr(%v)targetAddr(%v)", conn.LocalAddr(),
	//	conn.RemoteAddr(), targetAddress)
	req, err := http.NewRequest("CONNECT", targetAddress, nil)
	if err != nil {
		return fmt.Errorf("NewRequest target(%v) err:%v", targetAddress, err)
	}
	err = req.Write(conn)
	if err != nil {
		return fmt.Errorf("write to  target(%v) err:%v", targetAddress, err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		return fmt.Errorf(" target(%v) ReadResponse err:%v", targetAddress, err)
	}
	if resp == nil {
		return fmt.Errorf(" target(%v) resp == nil", targetAddress)
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to establish HTTP tunnel  target(%v)code(%v)", targetAddress, resp.StatusCode)
	}
	return nil
}
