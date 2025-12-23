/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"encoding/json"
	"fmt"
	"net"

	"techtradechain.com/techtradechain/common/v2/crypto/tls"
	"techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/net-libp2p/utils"
	"github.com/libp2p/go-libp2p-core/crypto"
)

type handShakeMsg struct {
	Certs    [][]byte
	CertsEnc [][]byte
}

type handShakeRes struct {
	remoteCerts [][]byte
	cert        []*x509.Certificate
	pk          crypto.PubKey
}

//  msgToRes .
//  @Description:
//  @param m
//  @return *handShakeRes
//  @return error
func msgToRes(m *handShakeMsg) (*handShakeRes, error) {
	if len(m.Certs) <= 0 {
		return nil, fmt.Errorf("len(m.Certs) <= 0")
	}
	var x509Cert []*x509.Certificate
	remoteCert, err := x509.ParseCertificate(m.Certs[0])
	if err != nil {
		return nil, fmt.Errorf("ParseCertificate failed: %s", err)
	}
	x509Cert = append(x509Cert, remoteCert)
	pk, err := utils.ParsePublicKeyToPubKey(remoteCert.PublicKey.ToStandardKey())
	if err != nil {
		return nil, fmt.Errorf("ParsePublicKeyToPubKey public key failed: %s", err)
	}
	if len(m.CertsEnc) > 0 {
		remoteCertEnc, err := x509.ParseCertificate(m.CertsEnc[0])
		if err != nil {
			return nil, fmt.Errorf("ParseCertificate failed: %s", err)
		}
		x509Cert = append(x509Cert, remoteCertEnc)
	}
	return &handShakeRes{cert: x509Cert, remoteCerts: m.Certs, pk: pk}, nil
}

//  makeHandShakeMsg .
//  @Description:
//  @param tlsCfg
//  @return *handShakeMsg
func makeHandShakeMsg(tlsCfg *tls.Config) *handShakeMsg {
	m := &handShakeMsg{
		Certs: tlsCfg.Certificates[0].Certificate,
	}
	if len(tlsCfg.Certificates) > 1 {
		m.CertsEnc = tlsCfg.Certificates[1].Certificate
	}
	return m
}

//  rawToHandMsg .
//  @Description:
//  @param raw
//  @return *handShakeMsg
//  @return error
func rawToHandMsg(raw []byte) (*handShakeMsg, error) {
	var msg handShakeMsg
	err := json.Unmarshal(raw, &msg)
	return &msg, err
}

//  handMsgToRaw .
//  @Description:
//  @param msg
//  @return []byte
func handMsgToRaw(msg *handShakeMsg) ([]byte, error) {
	handShakeRaw, err := json.Marshal(msg)
	return handShakeRaw, err
}

//  clientExchangeCert .
//  @Description:exchange cert
//  @param conn
//  @param tlsCfg
//  @return *handShakeRes
//  @return error
func clientExchangeCert(conn net.Conn, tlsCfg *tls.Config) (*handShakeRes, error) {
	// write
	m := makeHandShakeMsg(tlsCfg)
	handShakeRaw, err := handMsgToRaw(m)
	if err != nil {
		return nil, fmt.Errorf("handMsgToRaw failed: %v", err)
	}
	err = writeMsg(handShakeRaw, conn)
	if err != nil {
		return nil, fmt.Errorf("writeMsg failed: %v", err)
	}
	// read
	msgData, err := readMsg(conn)
	if err != nil {
		return nil, fmt.Errorf("readMsg failed: %v", err)
	}
	// parse
	remoteCertInfo, err := rawToHandMsg(msgData)
	if err != nil {
		return nil, fmt.Errorf("rawToHandMsg failed: %v", err)
	}
	res, err := msgToRes(remoteCertInfo)
	if err != nil {
		return nil, fmt.Errorf("parse cert failed: %v", err)
	}
	return res, nil
}

//  serverExchangeCert .
//  @Description:exchange cert
//  @param conn
//  @param tlsCfg
//  @return *handShakeRes
//  @return error
func serverExchangeCert(conn net.Conn, tlsCfg *tls.Config) (*handShakeRes, error) {
	// read
	var err error
	msgData, err := readMsg(conn)
	if err != nil {
		return nil, fmt.Errorf("readMsg failed: %v", err)
	}
	// parse
	remoteCertInfo, err := rawToHandMsg(msgData)
	if err != nil {
		return nil, fmt.Errorf("rawToHandMsg failed: %v", err)
	}
	res, err := msgToRes(remoteCertInfo)
	if err != nil {
		return nil, fmt.Errorf("parse cert failed: %v", err)
	}
	// write
	m := makeHandShakeMsg(tlsCfg)
	handShakeRaw, err := handMsgToRaw(m)
	if err != nil {
		return nil, fmt.Errorf("handMsgToRaw failed: %v", err)
	}
	err = writeMsg(handShakeRaw, conn)
	if err != nil {
		return nil, fmt.Errorf("writeMsg failed: %v", err)
	}
	return res, nil
}
