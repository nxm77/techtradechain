/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sm2

import "C"
import (
	"crypto/elliptic"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"

	"techtradechain.com/techtradechain/common/v2/opencrypto/gmssl/gmssl"
)

type publicKeyInfo struct {
	Raw       asn1.RawContent
	Algorithm pkix.AlgorithmIdentifier
	PublicKey asn1.BitString
}

// MarshalPublicKey public key conversion
func MarshalPublicKey(key *PublicKey) ([]byte, error) {
	if key == nil {
		return nil, errors.New("input SM2 public key is null")
	}
	var pkPem string
	var err error
	if len(key.pkPem) != 0 {
		pkPem = key.pkPem
	} else {
		pkPem, err = key.GetPEM()
		if err != nil {
			return nil, err
		}
	}
	p, _ := pem.Decode([]byte(pkPem))
	if p == nil {
		return nil, errors.New("invalid public key pem")
	}
	return p.Bytes, nil
}

// UnmarshalPublicKey unmarshl der publickey to PublicKey
func UnmarshalPublicKey(der []byte) (*PublicKey, error) {
	if der == nil {
		return nil, errors.New("input DER is null")
	}
	var pki publicKeyInfo
	if rest, err := asn1.Unmarshal(der, &pki); err != nil {
		return nil, err
	} else if len(rest) != 0 {
		return nil, errors.New("x509: trailing data after ASN.1 of public-key")
	}
	if !pki.Algorithm.Algorithm.Equal(utils.OidSM2) {
		return nil, fmt.Errorf("fail to unmarshal public key: curve is not SM2P256v1")
	}

	asn1Data := pki.PublicKey.RightAlign()
	x, y := elliptic.Unmarshal(utils.P256Sm2(), asn1Data)
	if x == nil || y == nil {
		return nil, errors.New("x509: failed to unmarshal elliptic curve point")
	}

	pkPem, err := PublicKeyDerToPEM(der)
	if err != nil {
		return nil, err
	}
	return PublicKeyFromPEM(pkPem)
}

func PublicKeyDerToPEM(der []byte) (string, error) {
	if der == nil {
		return "", errors.New("der is nil")
	}
	pemPK := pem.EncodeToMemory(
		&pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: der,
		})
	return string(pemPK), nil
}

func PublicKeyFromPEM(pkPEM string) (*PublicKey, error) {
	pk, err := gmssl.NewPublicKeyFromPEM(pkPEM)
	if err != nil {
		return nil, err
	}
	return &PublicKey{PublicKey: pk, pkPem: pkPEM}, nil
}

func PublicKeyToPEM(key *PublicKey) (string, error) {
	return key.GetPEM()
}

// MarshalPrivateKey private key conversion
func MarshalPrivateKey(key *PrivateKey) ([]byte, error) {
	if key == nil {
		return nil, errors.New("input SM2 private key is null")
	}
	var skPem string
	var err error
	if len(key.skPem) != 0 {
		skPem = key.skPem
	} else {
		skPem, err = PrivateKeyToPEM(key)
		if err != nil {
			return nil, err
		}
	}
	p, _ := pem.Decode([]byte(skPem))
	if p == nil {
		return nil, errors.New("invalid private key pem")
	}
	return p.Bytes, nil
}

func PrivateKeyDerToPEM(der []byte) (string, error) {
	if der == nil {
		return "", errors.New("input der is nil")
	}
	skPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: der,
	})

	//check
	if _, err := PrivateKeyFromPEM(string(skPEM), ""); err != nil {
		return "", err
	}

	return string(skPEM), nil
}

func PrivateKeyFromPEM(skPEM string, pass string) (*PrivateKey, error) {
	sk, err := gmssl.NewPrivateKeyFromPEM(skPEM, pass)
	if err != nil {
		return nil, err
	}

	pkPem, err := sk.GetPublicKeyPEM()
	if err != nil {
		return nil, err
	}
	pub, err := PublicKeyFromPEM(pkPem)
	if err != nil {
		return nil, err
	}
	return &PrivateKey{PrivateKey: sk, skPem: skPEM, Pub: *pub}, nil
}

func PrivateKeyToPEM(key *PrivateKey) (string, error) {
	return key.PrivateKey.GetUnencryptedPEM()
}

func UnmarshalPrivateKey(der []byte) (*PrivateKey, error) {
	return UnmarshalPrivateKeyWithCurve(nil, der)
}

func UnmarshalPrivateKeyWithCurve(namedCurveOID *asn1.ObjectIdentifier, der []byte) (*PrivateKey, error) {
	skPem, err := PrivateKeyDerToPEM(der)
	if err != nil {
		return nil, err
	}
	return PrivateKeyFromPEM(skPem, "")
}
