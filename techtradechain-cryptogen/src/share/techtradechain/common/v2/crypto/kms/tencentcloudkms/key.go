/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package tencentcloudkms

import (
	"crypto"
	"encoding/asn1"
	"encoding/base64"
	"fmt"
	"math/big"
	"time"

	"github.com/tjfoc/gmsm/sm2"
	"github.com/tjfoc/gmsm/sm3"

	"techtradechain.com/techtradechain/common/v2/crypto/asym/ecdsa"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/common/v2/json"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	kms "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/kms/v20190118"
)

type PrivateKey struct {
	kms      *kms.Client
	keyType  string
	keyId    string
	keyAlias string
	pubKey   bccrypto.PublicKey
	isPublic bool
}

func (sk *PrivateKey) Type() bccrypto.KeyType {
	return sk.PublicKey().Type()
}

func (sk *PrivateKey) Bytes() ([]byte, error) {
	keyConfig := KMSPrivateKeyConfig{
		KeyType:  sk.keyType,
		KeyId:    sk.keyId,
		KeyAlias: sk.keyAlias,
	}
	return json.Marshal(keyConfig)
}

func (sk *PrivateKey) String() (string, error) {
	skBytes, err := sk.Bytes()
	if err != nil {
		return "", err
	}
	return string(skBytes), nil
}

func (sk *PrivateKey) PublicKey() bccrypto.PublicKey {
	return sk.pubKey
}

// IsValidASN1Sig check if sig is asn1-encoded
func IsValidASN1Sig(sig []byte) ([]byte, bool) {
	var sigStruct ecdsa.Sig
	var err error
	if len(sig) == 64 {
		sigStruct.R = new(big.Int).SetBytes(sig[:32])
		sigStruct.S = new(big.Int).SetBytes(sig[32:])
		sig, err = asn1.Marshal(sigStruct)
		if err != nil {
			return nil, false
		}
		return sig, true
	}
	if _, err := asn1.Unmarshal(sig, &sigStruct); err != nil {
		return nil, false
	}
	return sig, true
}

func (sk *PrivateKey) Sign(data []byte) ([]byte, error) {
	msgBase64 := base64.StdEncoding.EncodeToString(data)

	request := kms.NewSignByAsymmetricKeyRequest()

	request.Algorithm = common.StringPtr(sk.keyType)
	request.MessageType = common.StringPtr(MODE_DIGEST)
	request.KeyId = common.StringPtr(sk.keyId)
	request.Message = common.StringPtr(msgBase64)
	// SM2DSA_ASN1 is compatible
	if !sk.isPublic && sk.Type() == bccrypto.SM2 {
		request.Algorithm = common.StringPtr("SM2DSA_ASN1")
	}

	for {
		response, err := sk.kms.SignByAsymmetricKey(request)
		if _, ok := err.(*errors.TencentCloudSDKError); ok {
			return nil, fmt.Errorf("KMS API error: %s", err)
		}
		if err != nil {
			return nil, err
		}
		sig, err := base64.StdEncoding.DecodeString(*(response.Response.Signature))
		if err != nil {
			return nil, err
		}
		//XXX, kms sometimes returns a signature which is not asn1 standard
		//check sig if asn1-encoded; convert RS to asn1-eccStruct
		if sig, ok := IsValidASN1Sig(sig); ok {
			return sig, nil
		}
		// retry if fail after 3ms
		time.Sleep(time.Millisecond * 3)
	}
}

func (sk *PrivateKey) SignWithOpts(msg []byte, opts *bccrypto.SignOpts) ([]byte, error) {
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

func (sk *PrivateKey) ToStandardKey() crypto.PrivateKey {
	return &Signer{sk}
}
