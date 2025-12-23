/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package sm2

import (
	"bytes"
	"crypto"
	"encoding/pem"
	"math/big"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/tencentsm"
	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"
	"github.com/pkg/errors"
)

type PrivateKey struct {
	pub  PublicKey
	D    *big.Int
	Text []byte
}

func (sk *PrivateKey) Bytes() ([]byte, error) {
	return MarshalPrivateKey(sk)
}

func (sk *PrivateKey) Type() bccrypto.KeyType {
	return bccrypto.SM2
}

func (sk *PrivateKey) String() (string, error) {
	skDER, err := sk.Bytes()
	if err != nil {
		return "", err
	}

	block := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: skDER,
	}

	buf := new(bytes.Buffer)
	if err = pem.Encode(buf, block); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (sk *PrivateKey) Sign(msg []byte) ([]byte, error) {
	return sk.signWithSM3(msg, []byte(utils.SM2_DEFAULT_USER_ID))
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
		return sk.signWithSM3(msg, []byte(uid))
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return nil, err
	}
	return sk.Sign(dgst)
}

func (sk *PrivateKey) PublicKey() bccrypto.PublicKey {
	return &sk.pub
}

func (sk *PrivateKey) ToStandardKey() crypto.PrivateKey {
	return &signer{PrivateKey: *sk}
}

func (sk *PrivateKey) DecryptWithMode(cipher []byte, mode tencentsm.SM2CipherMode) ([]byte, error) {
	if cipher == nil {
		return nil, errors.New("SM2 decrypt: ciphertext is null")
	}
	ctx := sk.pub.pool.GetCtx()
	defer sk.pub.pool.ReleaseCtx(ctx)

	var plainLen int
	plain := make([]byte, len(cipher))
	skByte := sk.Text
	ret := tencentsm.SM2DecryptWithMode(
		ctx,
		cipher[:],
		len(cipher),
		skByte[:],
		SM2_PRIVATE_KEY_SIZE,
		plain[:],
		&plainLen,
		mode,
	)
	if ret != 0 {
		return nil, errors.New("SM2: fail to decrypt")
	}
	return plain[0:plainLen], nil
}

func (sk *PrivateKey) signWithSM3(msg, id []byte) ([]byte, error) {
	return signWithMode(sk, msg, id, SM2_SIGNATURE_MODE_RS_ASN1)
}

var _ bccrypto.DecryptKey = (*PrivateKey)(nil)

func (sk PrivateKey) DecryptWithOpts(ciphertext []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	return sk.Decrypt(ciphertext)
}

func (sk PrivateKey) EncryptKey() bccrypto.EncryptKey {
	return &PublicKey{sk.pub.Curve, sk.pub.X, sk.pub.Y, sk.pub.Text, sk.pub.pool}
}

func (sk PrivateKey) Decrypt(cipher []byte) ([]byte, error) {
	return sk.DecryptWithMode(cipher, SM2_CIPHER_MODE_C1C3C2_ASN1)
}
