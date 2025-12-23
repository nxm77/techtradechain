/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package sm2

import (
	"crypto"
	"encoding/pem"
	"fmt"

	"github.com/tjfoc/gmsm/sm2"

	"github.com/tjfoc/gmsm/sm3"

	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"

	tjx509 "github.com/tjfoc/gmsm/x509"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"

	"techtradechain.com/techtradechain/common/v2/opencrypto/gmssl/gmssl"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
)

type PublicKey struct {
	*gmssl.PublicKey
	pkPem string
}

// PublicKey implements bccyrpto.PublicKey
var _ bccrypto.PublicKey = (*PublicKey)(nil)

func (pk *PublicKey) verifyWithSM3(msg, sig []byte, uid string) bool {
	dgst, err := pk.CalSM2Digest(uid, msg)
	if err != nil {
		return false
	}

	if err := pk.PublicKey.Verify("sm2sign", dgst, sig, nil); err != nil {
		return false
	}
	return true
}

func (pk *PublicKey) CalSM2Digest(uid string, msg []byte) ([]byte, error) {
	pkSM2, err := tjx509.ReadPublicKeyFromPem([]byte(pk.pkPem))
	if err != nil {
		return nil, err
	}

	if len(uid) == 0 {
		uid = bccrypto.CRYPTO_DEFAULT_UID
	}

	za, err := sm2.ZA(pkSM2, []byte(uid))
	if err != nil {
		return nil, fmt.Errorf("fail to create SM3 digest for msg [%v]", err)
	}
	e := sm3.New()
	e.Write(za)
	e.Write(msg)
	dgst := e.Sum(nil)

	return dgst, nil
}

func (pk *PublicKey) Bytes() ([]byte, error) {
	p, _ := pem.Decode([]byte(pk.pkPem))
	return p.Bytes, nil
}

func (pk *PublicKey) Type() bccrypto.KeyType {
	return bccrypto.SM2
}

func (pk *PublicKey) String() (string, error) {
	return pk.pkPem, nil
}

func (pk *PublicKey) Verify(msg []byte, sig []byte) (bool, error) {
	return pk.verifyWithSM3(msg, sig, utils.SM2_DEFAULT_USER_ID), nil
}

func (pk *PublicKey) VerifyWithOpts(msg []byte, sig []byte, opts *bccrypto.SignOpts) (bool, error) {
	if opts == nil {
		return pk.Verify(msg, sig)
	}
	if opts.Hash == bccrypto.HASH_TYPE_SM3 && pk.Type() == bccrypto.SM2 {
		uid := opts.UID
		if len(uid) == 0 {
			uid = bccrypto.CRYPTO_DEFAULT_UID
		}

		if sig == nil {
			return false, fmt.Errorf("nil signature")
		}
		return pk.verifyWithSM3(msg, sig, uid), nil
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return false, err
	}
	return pk.Verify(dgst, sig)
}

// ToStandardKey nolint
func (pk *PublicKey) ToStandardKey() crypto.PublicKey {
	der, err := MarshalPublicKey(pk)
	if err != nil {
		fmt.Println("failed to MarshalPublicKey, err = " + err.Error())
	}

	pub, err := tjx509.ParseSm2PublicKey(der)
	if err != nil {
		fmt.Println("failed to ParseSm2PublicKey, err = " + err.Error())
	}
	return pub
}

// PublicKey implements bccyrpto.PublicKey
var _ bccrypto.EncryptKey = (*PublicKey)(nil)

func (pk *PublicKey) EncryptWithOpts(data []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	return pk.Encrypt(data)
}

func (pk *PublicKey) Encrypt(plaintext []byte) ([]byte, error) {
	return pk.PublicKey.Encrypt("sm2encrypt-with-sm3", plaintext, nil)
}
