/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"crypto"
	"encoding/json"
	"errors"
	"net"

	"techtradechain.com/techtradechain/common/v2/crypto/tls"
	"techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/common/v2/random/uuid"
)

type akeMsg struct {
	R1 string //client random
	R2 string //server random
	S  []byte //sign
}

//  akeMsgToRaw .
//  @Description:
//  @param m
//  @return []byte
func akeMsgToRaw(m *akeMsg) ([]byte, error) {
	return json.Marshal(m)
}

//  rawToAkeMsg .
//  @Description:
//  @param raw
//  @return *akeMsg
//  @return error
func rawToAkeMsg(raw []byte) (*akeMsg, error) {
	var msg akeMsg
	err := json.Unmarshal(raw, &msg)
	return &msg, err
}

//  getPrivateKey .
//  @Description:
//  @param tlsCfg
//  @return crypto.PrivateKey
//  @return crypto.PrivateKey
func getPrivateKey(tlsCfg *tls.Config) (crypto.PrivateKey, crypto.PrivateKey) {
	if len(tlsCfg.Certificates) <= 0 {
		return nil, nil
	}
	signKey := tlsCfg.Certificates[0].PrivateKey
	var encKey crypto.PrivateKey
	if len(tlsCfg.Certificates) == 1 {
		encKey = tlsCfg.Certificates[0].PrivateKey
	} else {
		encKey = tlsCfg.Certificates[1].PrivateKey
	}
	return signKey, encKey
}

//  getPublicKey .
//  @Description:
//  @param cert
//  @return crypto.PublicKey
//  @return crypto.PublicKey
func getPublicKey(cert []*x509.Certificate) (crypto.PublicKey, crypto.PublicKey) {
	signKey := cert[0].PublicKey.ToStandardKey()
	var encKey crypto.PublicKey
	if len(cert) == 1 {
		encKey = cert[0].PublicKey.ToStandardKey()
	} else {
		encKey = cert[1].PublicKey.ToStandardKey()
	}
	return signKey, encKey
}

//  akeClientHello .
//  @Description:
//  @param conn
//  @param tlsCfg
//  @param r
//  @return error
func akeClientHello(conn net.Conn, tlsCfg *tls.Config, r *handShakeRes) error {
	// private key
	localPriSign, _ := getPrivateKey(tlsCfg)
	// public key
	remotePkSign, _ := getPublicKey(r.cert)
	//_ = remotePkEnc
	//_ = localPriEnc
	// send r1
	r1 := uuid.GetUUID()
	m := &akeMsg{
		R1: r1,
	}
	handShakeRaw, err := akeMsgToRaw(m)
	if err != nil {
		return err
	}
	//handShakeRaw, err = encrypt(remotePkEnc, handShakeRaw)
	//if err != nil {
	//	return err
	//}
	err = writeMsg(handShakeRaw, conn)
	if err != nil {
		return err
	}
	// recv sign(r1) r2
	msgData, err := readMsg(conn)
	if err != nil {
		return err
	}
	//msgData, err = decrypt(localPriEnc, msgData)
	//if err != nil {
	//	return err
	//}
	remoteAkeInfo, err := rawToAkeMsg(msgData)
	if err != nil {
		return err
	}
	// verify r1
	err = verify(remotePkSign, []byte(r1), remoteAkeInfo.S)
	if err != nil {
		return err
	}
	if remoteAkeInfo == nil || len(remoteAkeInfo.R2) == 0 {
		return errors.New("remoteAkeInfo == nil || remoteAkeInfo.R2 == nil")
	}
	// sign r2
	var r2Signed []byte
	r2Signed, err = sign(localPriSign, []byte(remoteAkeInfo.R2))
	if err != nil {
		return err
	}
	// response sign(r2)
	response := &akeMsg{
		S: r2Signed,
	}
	var responseRaw []byte
	responseRaw, err = akeMsgToRaw(response)
	if err != nil {
		return err
	}
	return writeMsg(responseRaw, conn)
}

//  akeServerHello .
//  @Description:
//  @param conn
//  @param tlsCfg
//  @param r
//  @return error
func akeServerHello(conn net.Conn, tlsCfg *tls.Config, r *handShakeRes) error {
	// private key
	localPriSign, _ := getPrivateKey(tlsCfg)
	// public key
	remotePkSign, _ := getPublicKey(r.cert)
	//_ = remotePkEnc
	//_ = localPriEnc
	// recv r1
	msgData, err := readMsg(conn)
	if err != nil {
		return err
	}
	//msgData, err = decrypt(localPriEnc, msgData)
	//if err != nil {
	//	return err
	//}
	remoteAkeInfo, err := rawToAkeMsg(msgData)
	if err != nil {
		return err
	}
	if remoteAkeInfo == nil || len(remoteAkeInfo.R1) == 0 {
		return errors.New("remoteAkeInfo == nil || remoteAkeInfo.R1 == nil")
	}
	// sign r1
	var r1Signed []byte
	r1Signed, err = sign(localPriSign, []byte(remoteAkeInfo.R1))
	if err != nil {
		return err
	}
	// send r2 sign(r1)
	r2 := uuid.GetUUID()
	m := &akeMsg{
		R2: r2,
		S:  r1Signed,
	}
	var responseRaw []byte
	responseRaw, err = akeMsgToRaw(m)
	if err != nil {
		return err
	}
	//responseRaw, err = encrypt(remotePkEnc, responseRaw)
	//if err != nil {
	//	return err
	//}
	err = writeMsg(responseRaw, conn)
	if err != nil {
		return err
	}
	//recv sign(r2)
	msgData, err = readMsg(conn)
	if err != nil {
		return err
	}
	remoteAkeInfo, err = rawToAkeMsg(msgData)
	if err != nil {
		return err
	}
	// verify sign(r2)
	return verify(remotePkSign, []byte(r2), remoteAkeInfo.S)
}
