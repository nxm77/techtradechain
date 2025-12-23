/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package ecdsa

import (
	"bytes"
	crypto2 "crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"github.com/btcsuite/btcd/btcec"
)

type PublicKey struct {
	K *ecdsa.PublicKey
}

func (pk *PublicKey) Bytes() ([]byte, error) {
	if pk.K == nil {
		return nil, fmt.Errorf("public key is nil")
	}

	if pk.Type() == crypto.ECC_Secp256k1 {
		rawKey := (*btcec.PublicKey)(pk.K).SerializeCompressed()
		return rawKey, nil
	}
	return x509.MarshalPKIXPublicKey(pk.K)
}

func (pk *PublicKey) Verify(digest []byte, sig []byte) (bool, error) {
	if sig == nil {
		return false, fmt.Errorf("nil signature")
	}

	sigStruct := &Sig{}
	if _, err := asn1.Unmarshal(sig, sigStruct); err != nil {
		return false, fmt.Errorf("fail to decode signature: [%v]", err)
	}

	if !ecdsa.Verify(pk.K, digest, sigStruct.R, sigStruct.S) {
		return false, fmt.Errorf("struct invalid ecdsa signature")
	}

	return true, nil
}

func (pk *PublicKey) VerifyWithOpts(msg []byte, sig []byte, opts *crypto.SignOpts) (bool, error) {
	if opts == nil {
		return pk.Verify(msg, sig)
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return false, err
	}
	return pk.Verify(dgst, sig)
}

func (pk *PublicKey) Type() crypto.KeyType {
	if pk.K != nil {
		switch pk.K.Curve {
		case elliptic.P256():
			return crypto.ECC_NISTP256
		case elliptic.P384():
			return crypto.ECC_NISTP384
		case elliptic.P521():
			return crypto.ECC_NISTP521
		case btcec.S256():
			return crypto.ECC_Secp256k1
		}
	}

	return -1
}

func (pk *PublicKey) String() (string, error) {

	pkDER, err := pk.Bytes()
	if err != nil {
		return "", err
	}

	if pk.Type() == crypto.ECC_Secp256k1 {
		return hex.EncodeToString(pkDER), nil
	}

	block := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pkDER,
	}

	buf := new(bytes.Buffer)
	if err = pem.Encode(buf, block); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (pk *PublicKey) ToStandardKey() crypto2.PublicKey {
	return pk.K
}
