/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package paillier

// nolint:staticcheck
import (
	"bytes"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"math/big"
)

const (
	defaultChecksumSize = 5
)

func AdjustPlaintextDomain(pk *PubKey, plaintext *big.Int) (*big.Int, error) {
	plaintext = new(big.Int).Mod(plaintext, pk.N)
	return plaintext, nil
}

func AdjustDecryptedDomain(pk *PubKey, plaintext *big.Int) (*big.Int, error) {
	limitUp := new(big.Int).Div(pk.N, big.NewInt(2))
	if new(big.Int).Mod(pk.N, big.NewInt(2)).Cmp(one) == 0 {
		limitUp = new(big.Int).Add(limitUp, one)
	}
	plaintext = new(big.Int).Mod(plaintext, pk.N)
	if plaintext.Cmp(limitUp) == 1 {
		plaintext = new(big.Int).Sub(plaintext, pk.N)
	}
	return plaintext, nil
}

// validatePrvKey is used to validate the private key
func validatePrvKey(key *PrvKey) error {
	if key == nil || key.p == nil || key.q == nil {
		return ErrInvalidPrivateKey
	}

	if err := validatePubKey(key.PubKey); err != nil {
		return ErrInvalidPrivateKey
	}
	return nil
}

// validatePubKey is used to validate the public key
func validatePubKey(key *PubKey) error {
	if key == nil || key.G == nil || key.N == nil || key.NSquared == nil {
		return ErrInvalidPublicKey
	}

	return nil
}

// validateCiphertext is used to validate Ciphertext
func validateCiphertext(cts ...*Ciphertext) error {
	for _, ct := range cts {
		if ct == nil || ct.Ct == nil || ct.Checksum == nil {
			return ErrInvalidCiphertext
		}
	}

	return nil
}

// validatePlaintext is used to validate the paillier Ciphertext of type big.Int
func validatePlaintext(paillierTexts ...*big.Int) error {
	for _, paillierText := range paillierTexts {
		if paillierText == nil {
			return ErrInvalidPlaintext
		}
	}
	return nil
}

// public key io
func GetPublicKeyHex(pk *PubKey) string {
	n := pk.N.Text(16)
	return n
}

func GetPublicKeyFromHex(content string) (*PubKey, error) {
	n, isOK := new(big.Int).SetString(content, 16)
	if !isOK {
		//return nil, errors.New("invalid string for Paillier public key: " + content)
		return nil, ErrInvalidPublicKey
	}
	return &PubKey{
		N:        n,
		NSquared: new(big.Int).Mul(n, n),
		G:        new(big.Int).Add(n, one),
	}, nil
}

// nolint: gosec
func WritePublicKeyToFile(pk *PubKey, file string) error {
	return ioutil.WriteFile(file, []byte(GetPublicKeyHex(pk)), 0644)
}

func ReadPublicKeyFromFile(file string) (*PubKey, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return GetPublicKeyFromHex(string(content))
}

// private key io
func GetPrivateKeyHex(sk *PrvKey) string {
	// TO DER
	pkDER, err := asn1.Marshal(
		struct {
			P      *big.Int
			Q      *big.Int
			PubStr string
		}{
			sk.p,
			sk.q,
			GetPublicKeyHex(sk.PubKey),
		})
	if err != nil {
		return ""
	}

	// PEM encode
	block := &pem.Block{
		Type:  "PAILLIER PRIVATE KEY",
		Bytes: pkDER,
	}

	buf := new(bytes.Buffer)
	if err = pem.Encode(buf, block); err != nil {
		return ""
	}

	return buf.String()
}

func GetPrivateKeyFromHex(content string) (*PrvKey, error) {
	temp := struct {
		P      *big.Int
		Q      *big.Int
		PubStr string
	}{}

	// PEM decode
	block, rest := pem.Decode([]byte(content))
	if len(rest) != 0 {
		return nil, ErrInvalidPrivateKey
	}

	// DER to struct
	_, err := asn1.Unmarshal(block.Bytes, &temp)
	if err != nil {
		return nil, ErrInvalidPrivateKey
	}

	p := temp.P
	q := temp.Q

	n := new(big.Int).Mul(p, q)
	pp := new(big.Int).Mul(p, p)
	qq := new(big.Int).Mul(q, q)
	return &PrvKey{
		PubKey: &PubKey{
			N:        n,
			NSquared: new(big.Int).Mul(n, n),
			G:        new(big.Int).Add(n, one),
		},
		p:         p,
		pp:        pp,
		pminusone: new(big.Int).Sub(p, one),
		q:         q,
		qq:        qq,
		qminusone: new(big.Int).Sub(q, one),
		pinvq:     new(big.Int).ModInverse(p, q),
		hp:        h(p, pp, n),
		hq:        h(q, qq, n),
		n:         n,
	}, nil
}

func WritePrivateKeyToFile(sk *PrvKey, file string) error {
	return WriteEncryptedPrivateKeyToFile(sk, file, "")
}

// nolint: gosec
func WriteEncryptedPrivateKeyToFile(sk *PrvKey, file, password string) error {
	if password == "" {
		return ioutil.WriteFile(file, []byte(GetPrivateKeyHex(sk)), 0644)
	}
	// TODO: implement secret key encryption here
	return ioutil.WriteFile(file, []byte(GetPrivateKeyHex(sk)), 0644)
}

func ReadPrivateKeyFromFile(file string) (*PrvKey, error) {
	return ReadEncryptedPrivateKeyFromFile(file, "")
}

func ReadEncryptedPrivateKeyFromFile(file, password string) (*PrvKey, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	if password == "" {
		return GetPrivateKeyFromHex(string(content))
	}
	// TODO: implement secret key decryption here
	return GetPrivateKeyFromHex(string(content))
}

// ciphertext io
func GetCiphertextHex(cipher string) (string, error) {
	c, isOk := new(big.Int).SetString(cipher, 10)
	if !isOk {
		return "", ErrInvalidCiphertext
	}
	return c.Text(16), nil
}

func GetCiphertextFromHex(content string) (string, error) {
	c, isOK := new(big.Int).SetString(content, 16)
	if !isOK {
		return "", errors.New("invalid string for paillier ciphertext: " + content)
	}
	return c.Text(10), nil
}

// nolint: gosec
func WriteCiphertextToFile(cipher string, file string) error {
	content, err := GetCiphertextHex(cipher)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(file, []byte(content), 0644)
}

func ReadCiphertextFromFile(file string) (string, error) {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return "", err
	}
	return GetCiphertextFromHex(string(content))
}
