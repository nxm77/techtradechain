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
	"crypto/elliptic"
	"encoding/pem"
	"fmt"
	"math/big"

	tjx509 "github.com/tjfoc/gmsm/x509"

	"github.com/pkg/errors"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/tencentsm"
	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"
)

type PublicKey struct {
	elliptic.Curve
	X, Y *big.Int
	Text []byte
	pool *CtxPool
}

func (pk *PublicKey) Bytes() ([]byte, error) {
	return MarshalPublicKey(pk)
}

func (pk *PublicKey) Type() bccrypto.KeyType {
	return bccrypto.SM2
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

func (pk *PublicKey) Verify(data []byte, sig []byte) (bool, error) {
	return pk.verifyWithMode(data, sig, []byte(utils.SM2_DEFAULT_USER_ID),
		SM2_SIGNATURE_MODE_RS_ASN1), nil
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

		return pk.verifyWithSM3(msg, []byte(uid), sig), nil
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

func (pk *PublicKey) verifyWithSM3(msg, id, sig []byte) bool {
	return pk.verifyWithMode(msg, sig, id, SM2_SIGNATURE_MODE_RS_ASN1)
}

func (pk *PublicKey) verifyWithMode(msg, sig []byte, id []byte, mode tencentsm.SM2SignMode) bool {
	ctx := pk.pool.GetCtx()
	defer pk.pool.ReleaseCtx(ctx)

	pkByte := pk.Text
	ret := tencentsm.SM2VerifyWithMode(
		ctx,
		msg,
		len(msg),
		id,
		len(id),
		sig,
		len(sig),
		pkByte,
		len(pkByte),
		mode)
	return ret == 0
}

func (pk PublicKey) EncryptWithMode(msg []byte, mode tencentsm.SM2CipherMode) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("SM2 encrypt: plaintext is null")
	}
	ctx := pk.pool.GetCtx()
	defer pk.pool.ReleaseCtx(ctx)

	var cipherLen int
	cipher := make([]byte, len(msg)+SM2_CIPHER_EXTRA_SIZE)
	pkByte := pk.Text
	ret := tencentsm.SM2EncryptWithMode(
		ctx,
		msg[:],
		len(msg),
		pkByte[:],
		SM2_PUBLIC_KEY_SIZE,
		cipher[:],
		&cipherLen,
		mode,
	)
	if ret != 0 {
		return nil, errors.New("SM2: fail to encrypt")
	}
	return cipher[0:cipherLen], nil
}

var _ bccrypto.EncryptKey = (*PublicKey)(nil)

func (pk PublicKey) Encrypt(msg []byte) ([]byte, error) {
	return pk.EncryptWithMode(msg, SM2_CIPHER_MODE_C1C3C2_ASN1)
}

func (pk PublicKey) EncryptWithOpts(data []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	return pk.Encrypt(data)
}
