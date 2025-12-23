/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package sm2

import (
	"bytes"
	crypto2 "crypto"
	"crypto/rand"
	"encoding/asn1"
	"encoding/pem"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	tjsm2 "github.com/tjfoc/gmsm/sm2"
	gmx509 "github.com/tjfoc/gmsm/x509"
)

type PublicKey struct {
	K *tjsm2.PublicKey
}

func (pk *PublicKey) Bytes() ([]byte, error) {
	if pk.K == nil {
		return nil, fmt.Errorf("public key is nil")
	}

	return gmx509.MarshalPKIXPublicKey(pk.K)
}

func (pk *PublicKey) Verify(digest []byte, sig []byte) (bool, error) {
	if sig == nil {
		return false, fmt.Errorf("nil signature")
	}

	sigStruct := &Sig{}
	if _, err := asn1.Unmarshal(sig, sigStruct); err != nil {
		return false, fmt.Errorf("fail to decode signature: [%v]", err)
	}

	if !tjsm2.Verify(pk.K, digest, sigStruct.R, sigStruct.S) {
		return false, fmt.Errorf("invalid sm2 signature")
	}

	return true, nil
}

func (pk *PublicKey) VerifyWithOpts(msg []byte, sig []byte, opts *crypto.SignOpts) (bool, error) {
	if opts == nil {
		return pk.Verify(msg, sig)
	}
	if opts.Hash == crypto.HASH_TYPE_SM3 && pk.Type() == crypto.SM2 {
		uid := opts.UID
		if len(uid) == 0 {
			uid = crypto.CRYPTO_DEFAULT_UID
		}

		if sig == nil {
			return false, fmt.Errorf("nil signature")
		}

		sigStruct := &Sig{}
		if _, err := asn1.Unmarshal(sig, sigStruct); err != nil {
			return false, fmt.Errorf("fail to decode signature: [%v]", err)
		}
		return tjsm2.Sm2Verify(pk.K, msg, []byte(uid), sigStruct.R, sigStruct.S), nil
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return false, err
	}
	return pk.Verify(dgst, sig)
}

func (pk *PublicKey) Type() crypto.KeyType {
	switch pk.K.Curve {
	case tjsm2.P256Sm2():
		return crypto.SM2
	}

	return -1
}

func (pk *PublicKey) String() (string, error) {

	pkDER, err := pk.Bytes()
	if err != nil {
		return "", err
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

func (pk *PublicKey) Encrypt(data []byte) ([]byte, error) {
	return pk.EncryptWithOpts(data, defaultSM2Opts)
}

func (pk *PublicKey) EncryptWithOpts(data []byte, opts *crypto.EncOpts) ([]byte, error) {
	if opts == nil || opts.EnableASN1 {
		return tjsm2.EncryptAsn1(pk.K, data, rand.Reader)
	}
	return tjsm2.Encrypt(pk.K, data, rand.Reader, tjsm2.C1C3C2)
}
