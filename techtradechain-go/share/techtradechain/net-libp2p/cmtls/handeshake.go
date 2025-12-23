/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"fmt"
	"net"

	"techtradechain.com/techtradechain/common/v2/crypto/tls"
	api "techtradechain.com/techtradechain/protocol/v2"
)

type handshakeVersion uint32

const (
	unknown       handshakeVersion = iota + 1
	versionLEv234                  //versionLEv234
	version235
)

//  checkHandshakeVersion .
//  @Description:
//  @param bufConn
//  @return handshakeVersion
//  @return error
func checkHandshakeVersion(bufConn *msgBufferConn) (handshakeVersion, error) {
	header := make([]byte, handShakeHeaderLen)
	_, err := bufConn.Peek(header)
	if err != nil {
		return unknown, err
	}
	if verifyHandShakeHead(header) {
		return version235, nil
	}
	return versionLEv234, nil
}

//  clientHandshake .
//  @Description:1.exchangeCert 2.ake check 3.VerifyPeerCertificate
//  @param conn
//  @param config
//  @param log
//  @return *handShakeRes
//  @return error
func clientHandshake(conn net.Conn, config *tls.Config, log api.Logger) (*handShakeRes, error) {
	if conn == nil {
		return nil, fmt.Errorf("conn is nil")
	}
	log.Info("clientHandshake begin:", conn.RemoteAddr().String())
	handShakeResult, err := clientExchangeCert(conn, config)
	if err != nil {
		return nil, fmt.Errorf("clientExchangeCert(%v) err:%v", conn.RemoteAddr().String(), err)
	}
	err = akeClientHello(conn, config, handShakeResult)
	if err != nil {
		return nil, fmt.Errorf("akeClientHello(%v) err:%v", conn.RemoteAddr().String(), err)
	}
	err = config.VerifyPeerCertificate(handShakeResult.remoteCerts, nil)
	if err != nil {
		return nil, fmt.Errorf("VerifyPeerCertificate(%v) err:%v", conn.RemoteAddr().String(), err)
	}
	log.Info("clientHandshake ok:", conn.RemoteAddr().String())
	return handShakeResult, nil
}

//  serverHandshake .
//  @Description:1.exchangeCert 2.ake check 3.VerifyPeerCertificate
//  @param conn
//  @param config
//  @param log
//  @return *handShakeRes
//  @return error
func serverHandshake(conn net.Conn, config *tls.Config, log api.Logger) (*handShakeRes, error) {
	if conn == nil {
		return nil, fmt.Errorf("conn is nil")
	}
	log.Info("serverHandshake begin:", conn.RemoteAddr().String())
	handShakeResult, err := serverExchangeCert(conn, config)
	if err != nil {
		return nil, fmt.Errorf("serverExchangeCert(%v) err:%v", conn.RemoteAddr().String(), err)
	}
	err = akeServerHello(conn, config, handShakeResult)
	if err != nil {
		return nil, fmt.Errorf("akeServerHello(%v) err:%v", conn.RemoteAddr().String(), err)
	}
	err = config.VerifyPeerCertificate(handShakeResult.remoteCerts, nil)
	if err != nil {
		return nil, fmt.Errorf("VerifyPeerCertificate:%v", err)
	}
	log.Info("serverHandshake ok:", conn.RemoteAddr().String())
	return handShakeResult, nil
}
