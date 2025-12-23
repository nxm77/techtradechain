/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package paillier

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
)

var (
	one = big.NewInt(1)
)

// ErrMessageTooLong is returned when attempting to encrypt a message which is
// too large for the size of the public key.
var ErrMessageTooLong = errors.New("paillier: message too long for Paillier public key size")
var ErrInvalidCiphertext = errors.New("paillier: invalid ciphertext")
var ErrInvalidPlaintext = errors.New("paillier: invalid plaintext")
var ErrInvalidPublicKey = errors.New("paillier: invalid public key")
var ErrInvalidPrivateKey = errors.New("paillier: invalid private key")
var ErrInvalidMismatch = errors.New("paillier: key mismatch")

// PubKey represents the public part of a Paillier key.
type PubKey struct {
	N        *big.Int // modulus
	G        *big.Int // n+1, since p and q are same length
	NSquared *big.Int
}

// PrvKey represents a Paillier key.
type PrvKey struct {
	*PubKey
	p         *big.Int
	pp        *big.Int
	pminusone *big.Int
	q         *big.Int
	qq        *big.Int
	qminusone *big.Int
	pinvq     *big.Int
	hp        *big.Int
	hq        *big.Int
	n         *big.Int
}

type Ciphertext struct {
	Ct       *big.Int
	Checksum []byte
}

func GenKey() (*PrvKey, error) {
	return generateKey(rand.Reader, 256)
}

// generateKey generates an Paillier keypair of the given bit size using the
// random source random (for example, crypto/rand.Reader).
func generateKey(random io.Reader, bits int) (*PrvKey, error) {
	// First, begin generation of p in the background.
	var p *big.Int
	var errChan = make(chan error, 1)
	go func() {
		var err error
		p, err = rand.Prime(random, bits/2)
		errChan <- err
	}()

	// Now, find a prime q in the foreground.
	q, err := rand.Prime(random, bits/2)
	if err != nil {
		return nil, err
	}

	// Wait for generation of p to complete successfully.
	if err := <-errChan; err != nil {
		return nil, err
	}

	n := new(big.Int).Mul(p, q)
	pp := new(big.Int).Mul(p, p)
	qq := new(big.Int).Mul(q, q)

	return &PrvKey{
		PubKey: &PubKey{
			N:        n,
			NSquared: new(big.Int).Mul(n, n),
			G:        new(big.Int).Add(n, one), // g = n + 1
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

// hp hq
func h(p *big.Int, pp *big.Int, n *big.Int) *big.Int {
	gp := new(big.Int).Mod(new(big.Int).Sub(one, n), pp)

	lp := l(gp, p)

	hp := new(big.Int).ModInverse(lp, p)
	return hp
}

func l(u *big.Int, n *big.Int) *big.Int {
	return new(big.Int).Div(new(big.Int).Sub(u, one), n)
}

// Encrypt encrypts a plain text represented as a byte array. The passed plain
// text MUST NOT be larger than the modulus of the passed public key.
func (key *PubKey) Encrypt(plainText *big.Int) (*Ciphertext, error) {
	if err := validatePubKey(key); err != nil {
		return nil, err
	}

	if err := validatePlaintext(plainText); err != nil {
		return nil, err
	}

	plaintext, err := AdjustPlaintextDomain(key, plainText)
	if err != nil {
		return nil, err
	}
	c, _, err := EncryptAndNonce(key, plaintext)
	if err != nil {
		return nil, err
	}

	checksum, err := key.bindingCtPubKey(c.Bytes())

	ct := &Ciphertext{
		Ct:       c,
		Checksum: checksum,
	}
	return ct, err
}

// EncryptAndNonce encrypts a plain text represented as a byte array, and in
// addition, returns the nonce used during encryption. The passed plain text
// MUST NOT be larger than the modulus of the passed public key.
func EncryptAndNonce(pubKey *PubKey, plainText *big.Int) (*big.Int, *big.Int, error) {
	r, err := rand.Int(rand.Reader, pubKey.N)
	if err != nil {
		return nil, nil, err
	}
	for new(big.Int).GCD(nil, nil, r, pubKey.N).Cmp(one) != 0 {
		r = new(big.Int).Mod(new(big.Int).Add(r, one), pubKey.N)
	}

	c, err := EncryptWithNonce(pubKey, r, plainText)
	if err != nil {
		return nil, nil, err
	}

	return c, r, nil
}

// EncryptWithNonce encrypts a plain text represented as a byte array using the
// provided nonce to perform encryption. The passed plain text MUST NOT be
// larger than the modulus of the passed public key.
func EncryptWithNonce(pubKey *PubKey, r *big.Int, pt *big.Int) (*big.Int, error) {
	if pubKey.N.Cmp(pt) < 1 { // N < m
		return nil, ErrMessageTooLong
	}

	// c = g^m * r^n mod n^2 = ((m*n+1) mod n^2) * r^n mod n^2
	n := pubKey.N
	c := new(big.Int).Mod(
		new(big.Int).Mul(
			new(big.Int).Mod(new(big.Int).Add(one, new(big.Int).Mul(pt, n)), pubKey.NSquared),
			new(big.Int).Exp(r, n, pubKey.NSquared),
		),
		pubKey.NSquared,
	)

	return c, nil
}

// Decrypt decrypts the passed cipher text.
func (key *PrvKey) Decrypt(cipherText *Ciphertext) (*big.Int, error) {
	if err := validatePrvKey(key); err != nil {
		return nil, err
	}

	if err := validateCiphertext(cipherText); err != nil {
		return nil, err
	}

	if key.NSquared.Cmp(cipherText.Ct) < 1 { // c > n^2
		return nil, ErrMessageTooLong
	}

	cp := new(big.Int).Exp(cipherText.Ct, key.pminusone, key.pp)
	lp := l(cp, key.p)
	mp := new(big.Int).Mod(new(big.Int).Mul(lp, key.hp), key.p)
	cq := new(big.Int).Exp(cipherText.Ct, key.qminusone, key.qq)
	lq := l(cq, key.q)

	mqq := new(big.Int).Mul(lq, key.hq)
	mq := new(big.Int).Mod(mqq, key.q)
	m := crt(mp, mq, key)

	plaintext, err := AdjustDecryptedDomain(key.PubKey, m)
	return plaintext, err
}

func crt(mp *big.Int, mq *big.Int, privKey *PrvKey) *big.Int {
	u := new(big.Int).Mod(new(big.Int).Mul(new(big.Int).Sub(mq, mp), privKey.pinvq), privKey.q)
	m := new(big.Int).Add(mp, new(big.Int).Mul(u, privKey.p))
	return new(big.Int).Mod(m, privKey.n)
}

func Neg(pk *PubKey, cipher *Ciphertext) (*Ciphertext, error) {
	cipher.Ct = new(big.Int).ModInverse(cipher.Ct, pk.NSquared)
	return cipher, nil
}

func (key *PrvKey) GetPubKey() (*PubKey, error) {
	if err := validatePrvKey(key); err != nil {
		return nil, err
	}

	return key.PubKey, nil
}

// Marshal encodes the PubKey as a byte slice.
func (key *PubKey) Marshal() ([]byte, error) {
	if err := validatePubKey(key); err != nil {
		return nil, err
	}
	// public key io
	return []byte(GetPublicKeyHex(key)), nil
}

// Unmarshal recovers the PubKey from an encoded byte slice.
func (key *PubKey) Unmarshal(pubKeyBytes []byte) error {
	k, err := GetPublicKeyFromHex(string(pubKeyBytes))
	if err != nil {
		return err
	}

	key.N = k.N
	key.NSquared = k.NSquared
	key.G = k.G

	return nil
}

func (ct *Ciphertext) Marshal() ([]byte, error) {
	if err := validateCiphertext(ct); err != nil {
		return nil, ErrInvalidCiphertext
	}

	ctBytes := ct.Ct.Bytes()
	return append(ct.Checksum, ctBytes...), nil
}

func (ct *Ciphertext) Unmarshal(ctBytes []byte) error {
	if ctBytes == nil {
		return ErrInvalidCiphertext
	}

	if ct.Ct == nil {
		ct.Ct = new(big.Int)
	}

	if len(ctBytes) < defaultChecksumSize {
		return fmt.Errorf("ctBytes(%s) is invalid, length = %d, need >= %d",
			string(ctBytes), len(ctBytes), defaultChecksumSize)
	}

	ct.Ct.SetBytes(ctBytes[defaultChecksumSize:])
	ct.Checksum = ctBytes[:defaultChecksumSize]
	return nil
}

// Marshal encodes the PrvKey as a byte slice.
func (key *PrvKey) Marshal() ([]byte, error) {
	if err := validatePrvKey(key); err != nil {
		return nil, err
	}

	tempBytes := []byte(GetPrivateKeyHex(key))

	return tempBytes, nil
}

// Unmarshal recovers the PrvKey from an encoded byte slice.
func (key *PrvKey) Unmarshal(prvKeyBytes []byte) error {
	if prvKeyBytes == nil {
		return ErrInvalidPrivateKey
	}

	k, err := GetPrivateKeyFromHex(string(prvKeyBytes))
	if err != nil {
		return ErrInvalidPrivateKey
	}

	key.PubKey = k.PubKey
	key.p = k.p
	key.pp = k.pp
	key.pminusone = k.pminusone
	key.q = k.q
	key.qq = k.qq
	key.qminusone = k.qminusone
	key.pinvq = k.pinvq
	key.hp = k.hp
	key.hq = k.hq
	key.n = k.n

	return nil
}

func (ct *Ciphertext) GetChecksum() ([]byte, error) {
	if err := validateCiphertext(ct); err != nil {
		return nil, err
	}

	return ct.Checksum, nil
}

func (ct *Ciphertext) GetCtBytes() ([]byte, error) {
	if err := validateCiphertext(ct); err != nil {
		return nil, err
	}

	return ct.Ct.Bytes(), nil
}

func (ct *Ciphertext) GetCtStr() (string, error) {
	if err := validateCiphertext(ct); err != nil {
		return "", err
	}

	return ct.Ct.String(), nil
}

// AddCiphertext homomorphically adds together two cipher texts.
// To do this we multiply the two cipher texts, upon decryption, the resulting
// plain text will be the sum of the corresponding plain texts.
func (key *PubKey) AddCiphertext(cipher1, cipher2 *Ciphertext) (*Ciphertext, error) {
	if err := validatePubKey(key); err != nil {
		return nil, err
	}

	if err := validateCiphertext(cipher1, cipher2); err != nil {
		return nil, err
	}

	if !key.checkOperand(cipher1, cipher2) {
		return nil, ErrInvalidMismatch
	}

	x := cipher1.Ct
	y := cipher2.Ct
	// x * y mod n^2
	c := new(big.Int).Mod(
		new(big.Int).Mul(x, y),
		key.NSquared,
	)

	return key.constructCiphertext(c)
}

// AddPlaintext homomorphically adds a passed constant to the encrypted integer
// (our cipher text). We do this by multiplying the constant with our
// ciphertext. Upon decryption, the resulting plain text will be the sum of
// the plaintext integer and the constant.
func (key *PubKey) AddPlaintext(cipher *Ciphertext, constant *big.Int) (*Ciphertext, error) {
	if err := validatePubKey(key); err != nil {
		return nil, err
	}

	if err := validateCiphertext(cipher); err != nil {
		return nil, err
	}

	if err := validatePlaintext(constant); err != nil {
		return nil, err
	}

	if !key.checkOperand(cipher) {
		return nil, ErrInvalidMismatch
	}

	c := cipher.Ct
	x := constant

	// c * g ^ x mod n^2
	c = new(big.Int).Mod(
		new(big.Int).Mul(c, new(big.Int).Exp(key.G, x, key.NSquared)),
		key.NSquared,
	)

	return key.constructCiphertext(c)
}

func (key *PubKey) SubCiphertext(cipher1, cipher2 *Ciphertext) (*Ciphertext, error) {
	if err := validatePubKey(key); err != nil {
		return nil, err
	}

	if err := validateCiphertext(cipher1, cipher2); err != nil {
		return nil, err
	}

	if !key.checkOperand(cipher1, cipher2) {
		return nil, ErrInvalidMismatch
	}

	c1 := cipher1.Ct
	c2 := cipher2.Ct
	c2Inversed := new(big.Int).ModInverse(c2, key.NSquared)
	c := new(big.Int).Mod(new(big.Int).Mul(c1, c2Inversed), key.NSquared)

	return key.constructCiphertext(c)
}

func (key *PubKey) SubPlaintext(cipher *Ciphertext, constant *big.Int) (*Ciphertext, error) {
	if err := validatePubKey(key); err != nil {
		return nil, err
	}

	if err := validateCiphertext(cipher); err != nil {
		return nil, err
	}

	if err := validatePlaintext(constant); err != nil {
		return nil, err
	}

	if !key.checkOperand(cipher) {
		return nil, ErrInvalidMismatch
	}

	plain := constant
	plain = new(big.Int).Mod(new(big.Int).Add(new(big.Int).Mul(plain, key.N), one), key.NSquared)
	c := cipher.Ct
	c = new(big.Int).Mod(new(big.Int).Mul(c, new(big.Int).ModInverse(plain, key.NSquared)), key.NSquared)

	return key.constructCiphertext(c)
}

func (key *PubKey) SubByConstant(pubKey *PubKey, cipher *Ciphertext, constant *big.Int) (*Ciphertext, error) {
	cipherNeg, err := Neg(pubKey, cipher)
	if err != nil {
		return nil, err
	}
	return key.AddPlaintext(cipherNeg, constant)
}

// NumMul homomorphically multiplies an encrypted integer (cipher text) by a
// constant. We do this by raising our cipher text to the power of the passed
// constant. Upon decryption, the resulting plain text will be the product of
// the plaintext integer and the constant.
func (key *PubKey) NumMul(cipher *Ciphertext, constant *big.Int) (*Ciphertext, error) {
	if err := validatePubKey(key); err != nil {
		return nil, err
	}

	if err := validateCiphertext(cipher); err != nil {
		return nil, err
	}

	if err := validatePlaintext(constant); err != nil {
		return nil, err
	}

	if !key.checkOperand(cipher) {
		return nil, ErrInvalidMismatch
	}

	c := new(big.Int).Exp(cipher.Ct, constant, key.NSquared)

	return key.constructCiphertext(c)
}

func (key *PubKey) constructCiphertext(ciphertext *big.Int) (*Ciphertext, error) {
	checksum, err := key.bindingCtPubKey(ciphertext.Bytes())
	if err != nil {
		return nil, err
	}

	ct := &Ciphertext{
		Ct:       ciphertext,
		Checksum: checksum,
	}

	return ct, nil
}

func (key *PubKey) bindingCtPubKey(ciphertext []byte) ([]byte, error) {
	pubKeyBytes, err := key.Marshal()
	if ciphertext == nil {
		return nil, ErrInvalidCiphertext
	}

	if err != nil {
		return nil, err
	}

	checksum := sha256.Sum256(append(pubKeyBytes, ciphertext[:]...))
	return checksum[:defaultChecksumSize], nil
}

func (key *PubKey) checkOperand(cts ...*Ciphertext) bool {
	for _, ct := range cts {
		if !key.ChecksumVerify(ct) {
			return false
		}
	}
	return true
}

// ChecksumVerify verifying public key ciphertext pairs
func (key *PubKey) ChecksumVerify(ct *Ciphertext) bool {
	if err := validatePubKey(key); err != nil {
		return false
	}

	if err := validateCiphertext(ct); err != nil {
		return false
	}

	keyBytes, err := key.Marshal()
	if err != nil {
		return false
	}

	ctBytes, err := ct.GetCtBytes()
	if err != nil {
		return false
	}

	currentChecksum, err := ct.GetChecksum()
	if err != nil {
		return false
	}

	checksum := sha256.Sum256(append(keyBytes, ctBytes...))
	return reflect.DeepEqual(checksum[:defaultChecksumSize], currentChecksum)
}
