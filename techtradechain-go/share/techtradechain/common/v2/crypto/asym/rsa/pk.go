/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package rsa

import (
	"bytes"
	crypto2 "crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
)

type PublicKey struct {
	K *rsa.PublicKey
}

func (pk *PublicKey) Bytes() ([]byte, error) {
	if pk.K == nil {
		return nil, fmt.Errorf("public key is nil")
	}

	return x509.MarshalPKCS1PublicKey(pk.K), nil
}

func (pk *PublicKey) Verify(data []byte, sig []byte) (bool, error) {
	hashed := sha256.Sum256(data)
	err := rsa.VerifyPKCS1v15(pk.K, crypto2.SHA256, hashed[:], sig)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (pk *PublicKey) VerifyWithOpts(data []byte, sig []byte, opts *crypto.SignOpts) (bool, error) {
	if opts == nil || opts.Hash == crypto.HASH_TYPE_SM3 {
		return pk.Verify(data, sig)
	}
	hashed, err := hash.Get(opts.Hash, data)
	if err != nil {
		return false, err
	}
	switch opts.EncodingType {
	case RSA_PSS:
		err = rsa.VerifyPSS(pk.K, crypto2.SHA256, hashed, sig, nil)
	default:
		err = rsa.VerifyPKCS1v15(pk.K, crypto2.Hash(opts.Hash), hashed[:], sig)
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (pk *PublicKey) Type() crypto.KeyType {
	if pk != nil && pk.K != nil {
		bitLen := pk.K.N.BitLen()
		if bitLen <= 512 {
			return crypto.RSA512
		} else if bitLen <= 1024 {
			return crypto.RSA1024
		} else if bitLen <= 2048 {
			return crypto.RSA2048
		} else if bitLen <= 3072 {
			return crypto.RSA3072
		}
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
	return pk.EncryptWithOpts(data, defaultRSAOpts)
}

func (pk *PublicKey) EncryptWithOpts(data []byte, opts *crypto.EncOpts) ([]byte, error) {
	switch opts.EncodingType {
	case RSA_OAEP:
		hashAlgo, err := hash.GetHashAlgorithm(opts.Hash)
		if err != nil {
			return nil, fmt.Errorf("RSA encryption fails: %v", err)
		}
		return rsa.EncryptOAEP(hashAlgo, rand.Reader, pk.ToStandardKey().(*rsa.PublicKey), data, opts.Label)
	case RSA_PKCS1:
		return rsa.EncryptPKCS1v15(rand.Reader, pk.ToStandardKey().(*rsa.PublicKey), data)
	default:
		return nil, fmt.Errorf("RSA encryption fails: unknown encoding type [%s]", opts.EncodingType)
	}
}
