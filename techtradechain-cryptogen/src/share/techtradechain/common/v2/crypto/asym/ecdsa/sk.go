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
	"crypto/rand"
	"crypto/x509"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"github.com/btcsuite/btcd/btcec"
)

type PrivateKey struct {
	K *ecdsa.PrivateKey
}

type Sig struct {
	R *big.Int `json:"r"`
	S *big.Int `json:"s"`
}

func (sk *PrivateKey) Bytes() ([]byte, error) {
	if sk.K == nil {
		return nil, fmt.Errorf("private key is nil")
	}

	if sk.Type() == crypto.ECC_Secp256k1 {
		rawKey := (*btcec.PrivateKey)(sk.K).Serialize()
		return rawKey, nil
	}
	return x509.MarshalECPrivateKey(sk.K)
}

func (sk *PrivateKey) PublicKey() crypto.PublicKey {
	return &PublicKey{K: &sk.K.PublicKey}
}

func (sk *PrivateKey) Sign(digest []byte) ([]byte, error) {
	var (
		r, s *big.Int
		err  error
	)

	r, s, err = ecdsa.Sign(rand.Reader, sk.K, digest[:])

	if err != nil {
		return nil, err
	}

	return asn1.Marshal(Sig{R: r, S: s})
}

func (sk *PrivateKey) SignWithOpts(msg []byte, opts *crypto.SignOpts) ([]byte, error) {
	if opts == nil {
		return sk.Sign(msg)
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return nil, err
	}
	return sk.Sign(dgst)
}

func (sk *PrivateKey) Type() crypto.KeyType {
	return sk.PublicKey().Type()
}

func (sk *PrivateKey) String() (string, error) {
	skDER, err := sk.Bytes()
	if err != nil {
		return "", err
	}

	if sk.Type() == crypto.ECC_Secp256k1 {
		return hex.EncodeToString(skDER), nil
	}

	block := &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: skDER,
	}

	buf := new(bytes.Buffer)
	if err = pem.Encode(buf, block); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (sk *PrivateKey) ToStandardKey() crypto2.PrivateKey {
	return sk.K
}

func New(keyType crypto.KeyType) (crypto.PrivateKey, error) {
	switch keyType {
	case crypto.ECC_Secp256k1:
		pri, err := ecdsa.GenerateKey(btcec.S256(), rand.Reader)
		if err != nil {
			return nil, err
		}

		return &PrivateKey{K: pri}, nil
	case crypto.ECC_NISTP256:
		pri, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, err
		}

		return &PrivateKey{K: pri}, nil
	case crypto.ECC_NISTP384:
		pri, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return nil, err
		}

		return &PrivateKey{K: pri}, nil
	case crypto.ECC_NISTP521:
		pri, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
		if err != nil {
			return nil, err
		}

		return &PrivateKey{K: pri}, nil
	}
	return nil, fmt.Errorf("wrong curve option")
}
