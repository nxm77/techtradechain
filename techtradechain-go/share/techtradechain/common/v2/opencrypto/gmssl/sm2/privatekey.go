/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package sm2

import (
	crypto2 "crypto"
	"encoding/pem"

	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/common/v2/opencrypto/gmssl/gmssl"
)

var _ bccrypto.PrivateKey = (*PrivateKey)(nil)

type PrivateKey struct {
	*gmssl.PrivateKey
	skPem string

	Pub PublicKey
}

func (sk *PrivateKey) Bytes() ([]byte, error) {
	p, _ := pem.Decode([]byte(sk.skPem))
	return p.Bytes, nil
}

func (sk *PrivateKey) Type() bccrypto.KeyType {
	return bccrypto.SM2
}

func (sk *PrivateKey) String() (string, error) {
	return sk.skPem, nil
}

func (sk *PrivateKey) PublicKey() bccrypto.PublicKey {
	return &sk.Pub
}

func (sk *PrivateKey) Sign(msg []byte) ([]byte, error) {
	return sk.signWithSM3(msg, utils.SM2_DEFAULT_USER_ID)
}

func (sk *PrivateKey) SignWithOpts(msg []byte, opts *bccrypto.SignOpts) ([]byte, error) {
	if opts == nil {
		return sk.Sign(msg)
	}
	if opts.Hash == bccrypto.HASH_TYPE_SM3 && sk.Type() == bccrypto.SM2 {
		uid := opts.UID
		if len(uid) == 0 {
			uid = bccrypto.CRYPTO_DEFAULT_UID
		}
		return sk.signWithSM3(msg, uid)
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return nil, err
	}
	return sk.Sign(dgst)

}

func (sk *PrivateKey) ToStandardKey() crypto2.PrivateKey {
	return &signer{PrivateKey: *sk}
}

// PrivateKey implements bccrypto.PrivateKey
func (sk *PrivateKey) signWithSM3(msg []byte, uid string) ([]byte, error) {
	dgst, err := sk.Pub.CalSM2Digest(uid, msg)
	if err != nil {
		return nil, err
	}

	return sk.PrivateKey.Sign("sm2sign", dgst, nil)
}

var _ bccrypto.DecryptKey = (*PrivateKey)(nil)

func (sk *PrivateKey) Decrypt(ciphertext []byte) ([]byte, error) {
	return sk.PrivateKey.Decrypt("sm2encrypt-with-sm3", ciphertext, nil)
}

func (sk *PrivateKey) DecryptWithOpts(ciphertext []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	return sk.Decrypt(ciphertext)
}

func (sk *PrivateKey) EncryptKey() bccrypto.EncryptKey {
	return &PublicKey{PublicKey: sk.Pub.PublicKey}
}
