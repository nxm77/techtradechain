/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package helper

import (
	goCrypto "crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"encoding/pem"
	"errors"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/helper/libp2pcrypto"
	"techtradechain.com/techtradechain/common/v2/helper/libp2ppeer"
	"github.com/btcsuite/btcd/btcec"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/tjfoc/gmsm/sm2"
	tjx509 "github.com/tjfoc/gmsm/x509"
)

// GetNodeUidFromAddr get the unique id of node from an addr. 从地址中截取出节点ID
func GetNodeUidFromAddr(addr string) (string, error) {
	maAddr, err := ma.NewMultiaddr(addr)
	if err != nil {
		return "", err
	}
	_, last := ma.SplitLast(maAddr)
	res, err := last.ValueForProtocol(ma.P_P2P)
	if err != nil {
		return "", fmt.Errorf("wrong address, %s", err.Error())
	}
	return res, nil
}

// GetLibp2pPeerIdFromCert create a peer.ID with pubKey that contains in cert.
func GetLibp2pPeerIdFromCert(certPemBytes []byte) (string, error) {
	var block *pem.Block
	block, _ = pem.Decode(certPemBytes)
	if block == nil {
		return "", errors.New("empty pem block")
	}
	if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
		return "", errors.New("not certificate pem")
	}

	return GetLibp2pPeerIdFromCertDer(block.Bytes)
}

// GetLibp2pPeerIdFromCertDer create a peer.ID with pubKey that contains in cert.
func GetLibp2pPeerIdFromCertDer(certDerBytes []byte) (string, error) {
	cert, err := tjx509.ParseCertificate(certDerBytes)
	if err != nil {
		return "", err
	}

	pubKey, err := ParseGoPublicKeyToPubKey(cert.PublicKey)
	if err != nil {
		return "", err
	}
	pid, err := libp2ppeer.IDFromPublicKey(pubKey)
	if err != nil {
		return "", err
	}
	return pid.Pretty(), err
}

// CreateLibp2pPeerIdWithPrivateKey create a peer.ID with crypto.PrivateKey.
func CreateLibp2pPeerIdWithPrivateKey(privateKey crypto.PrivateKey) (string, error) {
	return CreateLibp2pPeerIdWithPublicKey(privateKey.PublicKey())
}

// CreateLibp2pPeerIdWithPublicKey create a peer.ID with crypto.PublicKey.
func CreateLibp2pPeerIdWithPublicKey(publicKey crypto.PublicKey) (string, error) {
	pubKey, err := ParseGoPublicKeyToPubKey(publicKey.ToStandardKey())
	if err != nil {
		return "", err
	}
	pid, err := libp2ppeer.IDFromPublicKey(pubKey)
	if err != nil {
		return "", err
	}
	return pid.Pretty(), err
}

// ParseGoPublicKeyToPubKey parse a go crypto PublicKey to a libp2p crypto PubKey.
func ParseGoPublicKeyToPubKey(publicKey goCrypto.PublicKey) (libp2pcrypto.PubKey, error) {
	switch p := publicKey.(type) {
	case *ecdsa.PublicKey:
		if p.Curve == sm2.P256Sm2() {
			b, err := tjx509.MarshalPKIXPublicKey(p)
			if err != nil {
				return nil, err
			}
			pub, err := tjx509.ParseSm2PublicKey(b)
			if err != nil {
				return nil, err
			}
			return libp2pcrypto.NewSM2PublicKey(pub), nil
		}
		if p.Curve == btcec.S256() {
			return (*libp2pcrypto.Secp256k1PublicKey)(p), nil
		}
		return libp2pcrypto.NewECDSAPublicKey(p), nil

	case *sm2.PublicKey:
		return libp2pcrypto.NewSM2PublicKey(p), nil
	case *rsa.PublicKey:
		return libp2pcrypto.NewRsaPublicKey(*p), nil
	default:
		return nil, errors.New("unsupported public key type")
	}
}

// P2pAddressFormatVerify verify a node address format.
func P2pAddressFormatVerify(address string) bool {
	mA, err := ma.NewMultiaddr(address)
	if err != nil {
		return false
	}
	_, err = libp2ppeer.AddrInfoFromP2pAddr(mA)
	return err == nil
}
