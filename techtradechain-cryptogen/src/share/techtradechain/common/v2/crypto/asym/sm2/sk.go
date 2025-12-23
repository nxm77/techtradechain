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
	"crypto/aes"
	"crypto/cipher"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha512"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	tjsm2 "github.com/tjfoc/gmsm/sm2"
)

var defaultSM2Opts = &crypto.EncOpts{
	EncodingType: "",
	BlockMode:    "",
	EnableMAC:    false,
	Hash:         0,
	Label:        nil,
	EnableASN1:   true,
}

type PrivateKey struct {
	K *tjsm2.PrivateKey
}

type Sig struct {
	R *big.Int `json:"r"`
	S *big.Int `json:"s"`
}

func (sk *PrivateKey) Bytes() ([]byte, error) {
	if sk.K == nil {
		return nil, fmt.Errorf("private key is nil")
	}

	return MarshalPKCS8PrivateKey(sk.K)
}

func (sk *PrivateKey) PublicKey() crypto.PublicKey {
	return &PublicKey{K: &sk.K.PublicKey}
}

const (
	aesIV = "IV for <SM2> CTR"
)

type zr struct {
	io.Reader
}

func (z *zr) Read(dst []byte) (n int, err error) {
	for i := range dst {
		dst[i] = 0
	}
	return len(dst), nil
}

var zeroReader = &zr{}
var one = new(big.Int).SetInt64(1)

func randFieldElement(c elliptic.Curve, rand io.Reader) (k *big.Int, err error) {
	params := c.Params()
	b := make([]byte, params.BitSize/8+8)
	_, err = io.ReadFull(rand, b)
	if err != nil {
		return
	}
	k = new(big.Int).SetBytes(b)
	n := new(big.Int).Sub(params.N, one)
	k.Mod(k, n)
	k.Add(k, one)
	return
}

func SM2Sign(priv *tjsm2.PrivateKey, hash []byte) (r, s *big.Int, err error) {
	entropylen := (priv.Curve.Params().BitSize + 7) / 16
	if entropylen > 32 {
		entropylen = 32
	}
	entropy := make([]byte, entropylen)
	_, err = io.ReadFull(rand.Reader, entropy)
	if err != nil {
		return
	}

	// Initialize an SHA-512 hash context; digest ...
	md := sha512.New()
	md.Write(priv.D.Bytes()) // the private key,
	md.Write(entropy)        // the entropy,
	md.Write(hash)           // and the input hash;
	key := md.Sum(nil)[:32]  // and compute ChopMD-256(SHA-512),
	// which is an indifferentiable MAC.

	// Create an AES-CTR instance to use as a CSPRNG.
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}

	// Create a CSPRNG that xors a stream of zeros with
	// the output of the AES-CTR instance.
	csprng := cipher.StreamReader{
		R: zeroReader,
		S: cipher.NewCTR(block, []byte(aesIV)),
	}

	// See [NSA] 3.4.1
	c := priv.PublicKey.Curve
	n := c.Params().N
	if n.Sign() == 0 {
		return nil, nil, fmt.Errorf("zero parameter")
	}
	var k *big.Int
	e := new(big.Int).SetBytes(hash)
	for { // 调整算法细节以实现SM2
		for {
			k, err = randFieldElement(c, csprng)
			if err != nil {
				r = nil
				return
			}
			r, _ = priv.Curve.ScalarBaseMult(k.Bytes())
			r.Add(r, e)
			r.Mod(r, n)
			if r.Sign() != 0 {
				if t := new(big.Int).Add(r, k); t.Cmp(n) != 0 {
					break
				}
			}
		}
		rD := new(big.Int).Mul(priv.D, r)
		s = new(big.Int).Sub(k, rD)
		d1 := new(big.Int).Add(priv.D, one)
		d1Inv := new(big.Int).ModInverse(d1, n)
		s.Mul(s, d1Inv)
		s.Mod(s, n)
		if s.Sign() != 0 {
			break
		}
	}
	return
}

func (sk *PrivateKey) Sign(digest []byte) ([]byte, error) {
	var (
		r, s *big.Int
		err  error
	)

	r, s, err = SM2Sign(sk.K, digest[:])
	if err != nil {
		return nil, err
	}

	return asn1.Marshal(Sig{R: r, S: s})
}

func (sk *PrivateKey) SignWithOpts(msg []byte, opts *crypto.SignOpts) ([]byte, error) {
	if opts == nil {
		return sk.Sign(msg)
	}
	if opts.Hash == crypto.HASH_TYPE_SM3 && sk.Type() == crypto.SM2 {
		uid := opts.UID
		if len(uid) == 0 {
			uid = crypto.CRYPTO_DEFAULT_UID
		}

		r, s, err := tjsm2.Sm2Sign(sk.K, msg, []byte(uid), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("fail to sign with SM2-SM3: [%v]", err)
		}

		return asn1.Marshal(Sig{R: r, S: s})
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return nil, err
	}
	return sk.Sign(dgst)
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
		Type:  "PRIVATE KEY",
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
	return sk.DecryptWithOpts(ciphertext, defaultSM2Opts)
}

func (sk *PrivateKey) DecryptWithOpts(ciphertext []byte, opts *crypto.EncOpts) ([]byte, error) {
	if opts == nil || opts.EnableASN1 {
		return tjsm2.DecryptAsn1(sk.K, ciphertext)
	}
	return tjsm2.Decrypt(sk.K, ciphertext, tjsm2.C1C3C2)
}

func (sk *PrivateKey) EncryptKey() crypto.EncryptKey {
	return &PublicKey{&sk.K.PublicKey}
}

func New(keyType crypto.KeyType) (crypto.PrivateKey, error) {
	pri, err := tjsm2.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	return &PrivateKey{K: pri}, nil
}
