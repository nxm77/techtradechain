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

var defaultRSAOpts = &crypto.EncOpts{
	EncodingType: RSA_OAEP,
	BlockMode:    "",
	EnableMAC:    false,
	Hash:         crypto.HASH_TYPE_SHA256,
	Label:        nil,
}

type PrivateKey struct {
	keyType crypto.KeyType
	K       *rsa.PrivateKey
}

func New(keyType crypto.KeyType) (crypto.PrivateKey, error) {
	var bits crypto.BitsSize
	switch keyType {
	case crypto.RSA512:
		bits = crypto.BITS_SIZE_512
	case crypto.RSA1024:
		bits = crypto.BITS_SIZE_1024
	case crypto.RSA2048:
		bits = crypto.BITS_SIZE_2048
	case crypto.RSA3072:
		bits = crypto.BITS_SIZE_3072
	default:
		return nil, fmt.Errorf("unsupport RSA type")
	}

	priv, err := rsa.GenerateKey(rand.Reader, int(bits))
	if err != nil {
		return nil, err
	}

	return &PrivateKey{K: priv, keyType: keyType}, nil
}

func NewDecryptionKey(keyType crypto.KeyType) (crypto.DecryptKey, error) {
	priv, err := New(keyType)
	if err != nil {
		return nil, err
	}
	return priv.(crypto.DecryptKey), nil
}

func (sk *PrivateKey) Bytes() ([]byte, error) {
	if sk.K == nil {
		return nil, fmt.Errorf("private key is nil")
	}

	return x509.MarshalPKCS1PrivateKey(sk.K), nil
}

func (sk *PrivateKey) PublicKey() crypto.PublicKey {
	return &PublicKey{K: &sk.K.PublicKey}
}

func (sk *PrivateKey) Sign(data []byte) ([]byte, error) {
	hashed := sha256.Sum256(data)
	return rsa.SignPKCS1v15(rand.Reader, sk.K, crypto2.SHA256, hashed[:])
}

func (sk *PrivateKey) SignWithOpts(data []byte, opts *crypto.SignOpts) ([]byte, error) {
	if opts == nil || opts.Hash == crypto.HASH_TYPE_SM3 {
		return sk.Sign(data)
	}
	hashed, err := hash.Get(opts.Hash, data)
	if err != nil {
		return nil, err
	}
	switch opts.EncodingType {
	case RSA_PSS:
		return rsa.SignPSS(rand.Reader, sk.K, crypto2.SHA256, hashed, nil)
	default:
		return rsa.SignPKCS1v15(rand.Reader, sk.K, crypto2.Hash(opts.Hash), hashed[:])
	}
}

func (sk *PrivateKey) Type() crypto.KeyType {
	return sk.PublicKey().Type()
}

func (sk *PrivateKey) String() (string, error) {
	skDER, err := sk.Bytes()
	if err != nil {
		return "", err
	}

	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
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

func (sk *PrivateKey) Decrypt(ciphertext []byte) ([]byte, error) {
	return sk.DecryptWithOpts(ciphertext, defaultRSAOpts)
}

func (sk *PrivateKey) DecryptWithOpts(ciphertext []byte, opts *crypto.EncOpts) ([]byte, error) {

	// TODO switch encoding type
	switch opts.EncodingType {
	case RSA_OAEP:
		hashAlgo, err := hash.GetHashAlgorithm(opts.Hash)
		if err != nil {
			return nil, fmt.Errorf("RSA decryption fails: %v", err)
		}
		return rsa.DecryptOAEP(hashAlgo, rand.Reader, sk.ToStandardKey().(*rsa.PrivateKey), ciphertext, opts.Label)
	default:
		return nil, fmt.Errorf("RSA decryption fails: unknown encoding type [%s]", opts.EncodingType)
	}
}

func (sk *PrivateKey) EncryptKey() crypto.EncryptKey {
	return &PublicKey{K: &sk.K.PublicKey}
}
