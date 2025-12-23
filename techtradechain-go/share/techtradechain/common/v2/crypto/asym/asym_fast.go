//go:build linux
// +build linux

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package asym

import "C"
// nolint:staticcheck
import (
	crypto2 "crypto"
	ecdsa2 "crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	rsa2 "crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"path/filepath"
	"strings"

	"techtradechain.com/techtradechain/common/v2/crypto/asym/dilithium"
	"techtradechain.com/techtradechain/common/v2/crypto/engine"

	"github.com/pkg/errors"

	"techtradechain.com/techtradechain/common/v2/opencrypto"
	gmsm2 "techtradechain.com/techtradechain/common/v2/opencrypto/gmssl/sm2"
	tcsm2 "techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/sm2"

	"github.com/btcsuite/btcd/btcec"
	tjsm2 "github.com/tjfoc/gmsm/sm2"
	smx509 "github.com/tjfoc/gmsm/x509"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym/ecdsa"
	"techtradechain.com/techtradechain/common/v2/crypto/asym/rsa"
	"techtradechain.com/techtradechain/common/v2/crypto/asym/sm2"
)

const pemBegin = "-----BEGIN"

// 生成签名公私钥对
func GenerateKeyPair(keyType crypto.KeyType) (crypto.PrivateKey, error) {
	switch keyType {
	case crypto.SM2:
		if !engine.IsTls {
			switch engine.CryptoEngine {
			case opencrypto.GmSSL:
				return gmsm2.GenerateKeyPair()
			case opencrypto.TencentSM:
				return tcsm2.GenerateKeyPair()
			}
		}
		return sm2.New(keyType)
	case crypto.ECC_NISTP256, crypto.ECC_NISTP384, crypto.ECC_NISTP521, crypto.ECC_Secp256k1:
		return ecdsa.New(keyType)
	case crypto.RSA512, crypto.RSA1024, crypto.RSA2048, crypto.RSA3072:
		return rsa.New(keyType)
	case crypto.ECC_Ed25519:
		return nil, fmt.Errorf("unsupport signature algorithm")
	case crypto.DILITHIUM2:
		return dilithium.New(keyType)
	default:
		return nil, fmt.Errorf("wrong signature algorithm type")
	}
}

func GenerateKeyPairBytes(keyType crypto.KeyType) (sk, pk []byte, err error) {
	var priv crypto.PrivateKey
	if priv, err = GenerateKeyPair(keyType); err != nil {
		return
	}

	if sk, err = priv.Bytes(); err != nil {
		return
	}

	if pk, err = priv.PublicKey().Bytes(); err != nil {
		return
	}

	return sk, pk, nil
}

func GenerateKeyPairPEM(keyType crypto.KeyType) (sk string, pk string, err error) {
	var priv crypto.PrivateKey
	if priv, err = GenerateKeyPair(keyType); err != nil {
		return "", "", err
	}

	// Serialization for bitcoin signature key: encode ECC numbers with hex
	if sk, err = priv.String(); err != nil {
		return "", "", err
	}

	// Serialization for bitcoin signature key: encode ECC numbers with hex
	if pk, err = priv.PublicKey().String(); err != nil {
		return "", "", err
	}

	return sk, pk, nil
}

// Generate public-private key pair for encryption
func GenerateEncKeyPair(keyType crypto.KeyType) (crypto.DecryptKey, error) {
	switch keyType {
	case crypto.SM2:
		key, err := ecdsa.New(keyType)
		if err != nil {
			return nil, err
		}
		return key.(crypto.DecryptKey), nil
	case crypto.RSA512, crypto.RSA1024, crypto.RSA2048, crypto.RSA3072:
		return rsa.NewDecryptionKey(keyType)
	default:
		return nil, fmt.Errorf("unsupported encryption algorithm type")
	}
}

// ParsePrivateKey parse bytes to a private key.
func ParsePrivateKey(der []byte) (crypto2.PrivateKey, error) {
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}

	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}

	if key, err := smx509.ParsePKCS8UnecryptedPrivateKey(der); err == nil {
		return key, nil
	}

	// Serialization for bitcoin signature key: encode ECC numbers with hex
	Secp256k1Key, _ := btcec.PrivKeyFromBytes(btcec.S256(), der)
	key := Secp256k1Key.ToECDSA()
	return key, nil
}

func ParsePublicKey(der []byte) (crypto2.PublicKey, error) {
	if key, err := x509.ParsePKCS1PublicKey(der); err == nil {
		return key, nil
	}

	if key, err := x509.ParsePKIXPublicKey(der); err == nil {
		return key, nil
	}

	if key, err := smx509.ParseSm2PublicKey(der); err == nil {
		return key, nil
	}

	// Serialization for bitcoin signature key: encode ECC numbers with hex
	if key, err := btcec.ParsePubKey(der, btcec.S256()); err == nil {
		return key.ToECDSA(), nil
	}

	return nil, errors.New("failed to parse public key")
}

func PrivateKeyFromDER(der []byte) (crypto.PrivateKey, error) {
	if !engine.IsTls {
		switch engine.CryptoEngine {
		case opencrypto.GmSSL:
			if pri, err := gmsm2.UnmarshalPrivateKey(der); err == nil {
				return pri, nil
			}
		case opencrypto.TencentSM:
			if pri, err := tcsm2.UnmarshalPrivateKey(der); err == nil {
				return pri, nil
			}
		}
	}
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return &rsa.PrivateKey{K: key}, nil
	}

	if key, err := x509.ParseECPrivateKey(der); err == nil {
		if key.Curve == tjsm2.P256Sm2() {
			k := &tjsm2.PrivateKey{
				PublicKey: tjsm2.PublicKey{
					Curve: tjsm2.P256Sm2(),
					X:     key.X,
					Y:     key.Y,
				},
				D: key.D,
			}
			return &sm2.PrivateKey{K: k}, nil
		} else {
			return &ecdsa.PrivateKey{K: key}, nil
		}
	}

	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		switch k := key.(type) {
		case *ecdsa2.PrivateKey:
			return &ecdsa.PrivateKey{K: k}, nil
		case *tjsm2.PrivateKey:
			return &sm2.PrivateKey{K: k}, nil
		case *rsa2.PrivateKey:
			return &rsa.PrivateKey{K: k}, nil
		case crypto.PrivateKey:
			return k, nil
		default:
			return nil, fmt.Errorf("fail to parse private key, unrecognized key type [%T]", key)
		}
	}

	if key, err := smx509.ParsePKCS8UnecryptedPrivateKey(der); err == nil {
		return &sm2.PrivateKey{K: key}, nil
	}

	if key, err := dilithium.ParsePrivateKey(der); err == nil {
		return key, nil
	}

	Secp256k1Key, _ := btcec.PrivKeyFromBytes(btcec.S256(), der)
	key := Secp256k1Key.ToECDSA()
	return &ecdsa.PrivateKey{K: key}, nil
}

func PublicKeyFromDER(der []byte) (crypto.PublicKey, error) {
	if !engine.IsTls {
		switch engine.CryptoEngine {
		case opencrypto.GmSSL:
			if pub, err := gmsm2.UnmarshalPublicKey(der); err == nil {
				return pub, nil
			}
		case opencrypto.TencentSM:
			if pub, err := tcsm2.UnmarshalPublicKey(der); err == nil {
				return pub, nil
			}
		}
	}

	if key, err := x509.ParsePKCS1PublicKey(der); err == nil {
		return &rsa.PublicKey{K: key}, nil
	}

	if key, err := x509.ParsePKIXPublicKey(der); err == nil {
		switch key := key.(type) {
		case *rsa2.PublicKey:
			return &rsa.PublicKey{K: key}, nil
		case *ecdsa2.PublicKey:
			return &ecdsa.PublicKey{K: key}, nil
		case *tjsm2.PublicKey:
			return &sm2.PublicKey{K: key}, nil
		case crypto.PublicKey:
			return key, nil
		default:
			return nil, fmt.Errorf("unsupported public key type [%T]", key)
		}
	}

	if key, err := smx509.ParseSm2PublicKey(der); err == nil {
		return &sm2.PublicKey{K: key}, nil
	}

	if key, err := btcec.ParsePubKey(der, btcec.S256()); err == nil {
		return &ecdsa.PublicKey{K: key.ToECDSA()}, nil
	}

	if key, err := dilithium.ParsePubKey(der); err == nil {
		return key, nil
	}

	return nil, errors.New("failed to parse public key")
}

func PrivateKeyFromPEM(raw []byte, pwd []byte) (crypto.PrivateKey, error) {
	var err error

	if len(raw) <= 0 {
		return nil, errors.New("PEM is nil")
	}

	if !strings.Contains(string(raw), pemBegin) {
		var keyBytes []byte
		keyBytes, err = hex.DecodeString(string(raw))
		if err != nil {
			return nil, fmt.Errorf("fail to decode public key: [%v]", err)
		}
		return PrivateKeyFromDER(keyBytes)
	}

	block, _ := pem.Decode(raw)
	if block == nil {
		return PrivateKeyFromDER(raw)
	}

	plain := block.Bytes
	// TODO:
	// nolint: staticcheck
	if x509.IsEncryptedPEMBlock(block) {
		if len(pwd) <= 0 {
			return nil, errors.New("missing password for encrypted PEM")
		}

		// nolint: staticcheck
		plain, err = x509.DecryptPEMBlock(block, pwd)
		if err != nil {
			return nil, fmt.Errorf("fail to decrypt PEM: [%s]", err)
		}
	}

	return PrivateKeyFromDER(plain)
}

func PublicKeyFromPEM(raw []byte) (crypto.PublicKey, error) {
	if len(raw) <= 0 {
		return nil, errors.New("PEM is nil")
	}

	if !strings.Contains(string(raw), pemBegin) {
		keyBytes, err := hex.DecodeString(string(raw))
		if err != nil {
			return nil, fmt.Errorf("fail to decode public key: [%v]", err)
		}
		return PublicKeyFromDER(keyBytes)
	}

	block, _ := pem.Decode(raw)
	if block == nil {
		return PublicKeyFromDER(raw)
	}

	return PublicKeyFromDER(block.Bytes)
}

func Sign(sk interface{}, data []byte) ([]byte, error) {
	var (
		err        error
		r, s       *big.Int
		keyBytes   []byte
		signedData []byte
	)

	keyBytes, err = loadKeyBytes(sk)
	if err != nil {
		return nil, err
	}
	//try to parse private key if crypto engine is set
	if !engine.IsTls {
		switch engine.CryptoEngine {
		case opencrypto.GmSSL:
			if pri, e := gmsm2.UnmarshalPrivateKey(keyBytes); e == nil {
				return pri.Sign(data)
			}
		case opencrypto.TencentSM:
			if pri, e := tcsm2.UnmarshalPrivateKey(keyBytes); e == nil {
				return pri.Sign(data)
			}
		}
	}

	key, err := ParsePrivateKey(keyBytes)
	if err != nil {
		return nil, err
	}

	switch key := key.(type) {
	case *ecdsa2.PrivateKey:
		if r, s, err = ecdsa2.Sign(rand.Reader, key, data); err != nil {
			return nil, err
		}
		return asn1.Marshal(ecdsa.Sig{R: r, S: s})

	case *tjsm2.PrivateKey:
		if r, s, err = sm2SignWithoutHash(key, data); err != nil {
			return nil, err
		}
		return asn1.Marshal(ecdsa.Sig{R: r, S: s})

	case *rsa2.PrivateKey:
		hashed := sha256.Sum256(data)
		if signedData, err = rsa2.SignPKCS1v15(rand.Reader, key, crypto2.SHA256, hashed[:]); err != nil {
			return nil, err
		}
		return signedData, nil
	default:
		return nil, fmt.Errorf("fail to sign: unsupported algorithm")
	}
}

func Verify(pk interface{}, data, sig []byte) (bool, error) {
	if sig == nil {
		return false, fmt.Errorf("nil signature")
	}

	var (
		err      error
		keyBytes []byte
	)

	keyBytes, err = loadKeyBytes(pk)
	if err != nil {
		return false, err
	}

	//try to parse public key if crypto engine is set
	if !engine.IsTls {
		switch engine.CryptoEngine {
		case opencrypto.GmSSL:
			if pri, e := gmsm2.UnmarshalPublicKey(keyBytes); e == nil {
				return pri.Verify(data, sig)
			}
		case opencrypto.TencentSM:
			if pri, e := tcsm2.UnmarshalPublicKey(keyBytes); e == nil {
				return pri.Verify(data, sig)
			}
		}
	}

	key, err := ParsePublicKey(keyBytes)
	if err != nil {
		return false, err
	}

	if err := verifyDataSignWithPubKey(key, data, sig); err != nil {
		return false, err
	}

	return true, nil
}

var one = new(big.Int).SetInt64(1)

func randFieldElement(c elliptic.Curve, random io.Reader) (k *big.Int, err error) {
	if random == nil {
		random = rand.Reader //If there is no external trusted random source,please use rand.Reader to instead of it.
	}
	params := c.Params()
	b := make([]byte, params.BitSize/8+8)
	_, err = io.ReadFull(random, b)
	if err != nil {
		return
	}
	k = new(big.Int).SetBytes(b)
	n := new(big.Int).Sub(params.N, one)
	k.Mod(k, n)
	k.Add(k, one)
	return
}

func sm2SignWithoutHash(priv *tjsm2.PrivateKey, digest []byte) (r, s *big.Int, err error) {
	e := new(big.Int).SetBytes(digest)
	c := priv.PublicKey.Curve
	N := c.Params().N
	if N.Sign() == 0 {
		return nil, nil, errors.New("zero parameter")
	}
	var k *big.Int
	for { // 调整算法细节以实现SM2
		for {
			k, err = randFieldElement(c, rand.Reader)
			if err != nil {
				r = nil
				return
			}
			r, _ = priv.Curve.ScalarBaseMult(k.Bytes())
			r.Add(r, e)
			r.Mod(r, N)
			if r.Sign() != 0 {
				if t := new(big.Int).Add(r, k); t.Cmp(N) != 0 {
					break
				}
			}

		}
		rD := new(big.Int).Mul(priv.D, r)
		s = new(big.Int).Sub(k, rD)
		d1 := new(big.Int).Add(priv.D, one)
		d1Inv := new(big.Int).ModInverse(d1, N)
		s.Mul(s, d1Inv)
		s.Mod(s, N)
		if s.Sign() != 0 {
			break
		}
	}
	return
}

func loadKeyBytes(key interface{}) ([]byte, error) {
	var keyBytes []byte
	switch k := key.(type) {
	case string:
		if strings.Contains(k, pemBegin) {
			der, _ := pem.Decode([]byte(k))
			keyBytes = der.Bytes
		} else {
			var err error
			keyBytes, err = hex.DecodeString(k)
			if err != nil {
				return nil, err
			}
		}
	case []byte:
		keyBytes = k
	default:
		return nil, errors.New("invalid key format")
	}
	return keyBytes, nil
}

func verifyDataSignWithPubKey(key crypto2.PublicKey, data, sig []byte) error {
	switch key := key.(type) {
	case *ecdsa2.PublicKey:
		sigStruct := &ecdsa.Sig{}
		if _, err := asn1.Unmarshal(sig, sigStruct); err != nil {
			return err
		}

		if !ecdsa2.Verify(key, data, sigStruct.R, sigStruct.S) {
			return fmt.Errorf("string invalid ecdsa signature")
		}
	case *tjsm2.PublicKey:
		sigStruct := &ecdsa.Sig{}
		if _, err := asn1.Unmarshal(sig, sigStruct); err != nil {
			return err
		}

		if !tjsm2.Verify(key, data, sigStruct.R, sigStruct.S) {
			return fmt.Errorf("invalid sm2 signature")
		}
	case *rsa2.PublicKey:
		hashed := sha256.Sum256(data)
		err := rsa2.VerifyPKCS1v15(key, crypto2.SHA256, hashed[:], sig)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("fail to verify: unsupported algorithm")
	}
	return nil
}

func WriteFile(keyType crypto.KeyType, filePath string) error {
	sk, pk, err := GenerateKeyPairPEM(keyType)
	if err != nil {
		return err
	}

	skPath := filepath.Join(filePath, "node.key")
	if err = ioutil.WriteFile(skPath, []byte(sk), 0644); err != nil {
		return fmt.Errorf("save sk failed, %s", err)
	}

	pkPath := filepath.Join(filePath, "node.crt")
	if err = ioutil.WriteFile(pkPath, []byte(pk), 0644); err != nil {
		return fmt.Errorf("save pk failed, %s", err)
	}

	return nil
}

func ParseSM2PublicKey(asn1Data []byte) (*tjsm2.PublicKey, error) {
	if asn1Data == nil {
		return nil, errors.New("fail to unmarshal public key: public key is empty")
	}

	x, y := elliptic.Unmarshal(tjsm2.P256Sm2(), asn1Data)
	if x == nil {
		return nil, errors.New("x509: failed to unmarshal elliptic curve point")
	}

	pk := tjsm2.PublicKey{
		Curve: tjsm2.P256Sm2(),
		X:     x,
		Y:     y,
	}
	return &pk, nil
}
