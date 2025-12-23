/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sdf

import (
	"crypto"
	"fmt"
	"io"
	"strconv"

	"techtradechain.com/techtradechain/common/v2/crypto/hsm"

	"github.com/tjfoc/gmsm/sm3"

	"github.com/tjfoc/gmsm/sm2"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	bcsm2 "techtradechain.com/techtradechain/common/v2/crypto/asym/sm2"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"github.com/pkg/errors"
)

type ecdsaPrivateKey struct {
	priv *sdfEcdsaPrivateKey
}

func (e ecdsaPrivateKey) Public() crypto.PublicKey {
	return e.priv.pubKey.ToStandardKey()
}

func (e ecdsaPrivateKey) Sign(rand io.Reader, digest []byte, opts crypto.SignerOpts) (signature []byte, err error) {
	return e.priv.SignWithOpts(digest, &bccrypto.SignOpts{Hash: bccrypto.HASH_TYPE_SM3})
}

// sdfEcdsaPrivateKey represents pkcs11 ecdsa/sm2 private key
type sdfEcdsaPrivateKey struct {
	sdfHandle *SDFHandle
	pubKey    bccrypto.PublicKey
	keyId     uint
	keyPwd    []byte
	keyType   SDFKeyType

	signer crypto.Signer
}

// NewPrivateKey returns a sdf ec or sm2 private key
func NewPrivateKey(sdf *SDFHandle, keyId string, keyPwd []byte, tp bccrypto.KeyType) (bccrypto.PrivateKey, error) {
	if sdf == nil || len(keyId) == 0 {
		return nil, errors.New("Invalid parameter, sdfHandle or keyId is nil")
	}

	keyType := convertToSDFKeyType(tp)

	session, err := sdf.getSession()
	if err != nil {
		return nil, err
	}
	defer sdf.returnSession(err, session)

	//check keyId
	keyIndex, err := strconv.Atoi(keyId)
	if err != nil {
		return nil, err
	}

	{
		/*
			check pwd
			this depends on HSM vendors :-(
		*/
		accessKeyId, need := hsm.GetHSMAdapter("").SDF_GetSM2KeyAccessRight(keyIndex)
		if need {
			err = sdf.ctx.SDFGetPrivateKeyAccessRight(session, uint(accessKeyId), keyPwd, uint(len(keyPwd)))
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to geGetPrivateKeyAccessRight, accessKeyId = %d", accessKeyId)
			}
		}
	}

	//export pub
	pub, err := sdf.ExportECDSAPublicKey(SDFKey{uint(keyIndex), keyPwd, SM2})
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to ExportECDSAPublicKey, keyId = %d", keyIndex)
	}

	var bcPubKey bccrypto.PublicKey
	switch keyType {
	case SM2:
		bcPubKey = &bcsm2.PublicKey{K: pub.(*sm2.PublicKey)}
	default:
		return nil, errors.New("unknown key type, keyType = " + string(keyType))
	}

	sdfPrivateKey := &sdfEcdsaPrivateKey{
		sdfHandle: sdf,
		pubKey:    bcPubKey,
		keyId:     uint(keyIndex),
		keyPwd:    keyPwd,
		keyType:   keyType,
	}

	sdfPrivateKey.signer = &ecdsaPrivateKey{sdfPrivateKey}

	return sdfPrivateKey, nil
}

func (sk *sdfEcdsaPrivateKey) Type() bccrypto.KeyType {
	return sk.PublicKey().Type()
}

func (sk *sdfEcdsaPrivateKey) Bytes() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", sk.keyId)), nil
}

func (sk *sdfEcdsaPrivateKey) String() (string, error) {
	return fmt.Sprintf("%d", sk.keyId), nil
}

func (sk *sdfEcdsaPrivateKey) PublicKey() bccrypto.PublicKey {
	return sk.pubKey
}

func (sk *sdfEcdsaPrivateKey) Sign(data []byte) ([]byte, error) {
	switch sk.Type() {
	case bccrypto.SM2:
		return sk.sdfHandle.ECCInternalSign(SDFKey{sk.keyId, sk.keyPwd, sk.keyType}, data)
	default:
		return nil, errors.New("Not supported")
	}
}

func (sk *sdfEcdsaPrivateKey) SignWithOpts(msg []byte, opts *bccrypto.SignOpts) ([]byte, error) {
	if opts == nil {
		return sk.Sign(msg)
	}
	if opts.Hash == bccrypto.HASH_TYPE_SM3 && sk.Type() == bccrypto.SM2 {
		pkSM2, ok := sk.PublicKey().ToStandardKey().(*sm2.PublicKey)
		if !ok {
			return nil, fmt.Errorf("SM2 private key does not match the type it claims")
		}
		uid := opts.UID
		if len(uid) == 0 {
			uid = bccrypto.CRYPTO_DEFAULT_UID
		}

		za, err := sm2.ZA(pkSM2, []byte(uid))
		if err != nil {
			return nil, fmt.Errorf("PKCS11 error: fail to create SM3 digest for msg [%v]", err)
		}
		e := sm3.New()
		e.Write(za)
		e.Write(msg)
		dgst := e.Sum(nil)[:32]

		return sk.Sign(dgst)
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return nil, err
	}
	return sk.Sign(dgst)
}

func (sk *sdfEcdsaPrivateKey) ToStandardKey() crypto.PrivateKey {
	return sk.signer
}
