/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/pkg/errors"
	"github.com/tjfoc/gmsm/sm2"

	"github.com/miekg/pkcs11"
)

// GenerateRandom generate
func (p11 *P11Handle) GenerateRandom(length int) ([]byte, error) {
	session, err := p11.getSession()
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	return p11.ctx.GenerateRandom(session, length)
}

// Decrypt decrypts the input with a given mechanism.
func (p11 *P11Handle) Decrypt(obj pkcs11.ObjectHandle, mech *pkcs11.Mechanism, cipher []byte) ([]byte, error) {
	session, err := p11.getSession()
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	err = p11.ctx.DecryptInit(session, []*pkcs11.Mechanism{mech}, obj)
	if err != nil {
		return nil, err
	}
	out, err := p11.ctx.Decrypt(session, cipher)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Sign signs the input with a given mechanism.
func (p11 *P11Handle) Sign(obj pkcs11.ObjectHandle, mech *pkcs11.Mechanism, msg []byte) ([]byte, error) {
	session, err := p11.getSession()
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	err = p11.ctx.SignInit(session, []*pkcs11.Mechanism{mech}, obj)
	if err != nil {
		return nil, err
	}
	out, err := p11.ctx.Sign(session, msg)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Verify verifies a signature over a message with a given mechanism.
func (p11 *P11Handle) Verify(obj pkcs11.ObjectHandle, mech *pkcs11.Mechanism, msg, sig []byte) error {
	session, err := p11.getSession()
	if err != nil {
		return fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	err = p11.ctx.VerifyInit(session, []*pkcs11.Mechanism{mech}, obj)
	if err != nil {
		return err
	}
	err = p11.ctx.Verify(session, msg, sig)
	if err != nil {
		return err
	}
	return nil
}

// Encrypt encrypts a plaintext with a given mechanism.
func (p11 *P11Handle) Encrypt(obj pkcs11.ObjectHandle, mech *pkcs11.Mechanism, plain []byte) ([]byte, error) {
	session, err := p11.getSession()
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	err = p11.ctx.EncryptInit(session, []*pkcs11.Mechanism{mech}, obj)
	if err != nil {
		return nil, err
	}
	out, err := p11.ctx.Encrypt(session, plain)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GenKeyPair returns asym keypair
func (p11 *P11Handle) GenKeyPair(mech *pkcs11.Mechanism, privAttrs,
	pubAttrs []*pkcs11.Attribute) (pri, pub *pkcs11.ObjectHandle, err error) {
	session, err := p11.getSession()
	if err != nil {
		return nil, nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	pubHandle, privHandle, err := p11.ctx.GenerateKeyPair(session, []*pkcs11.Mechanism{mech}, pubAttrs, privAttrs)
	if err != nil {
		return nil, nil, err
	}
	return &privHandle, &pubHandle, nil
}

// GenerateKey returns sym key
func (p11 *P11Handle) GenerateKey(mech *pkcs11.Mechanism, attrs []*pkcs11.Attribute) (*pkcs11.ObjectHandle, error) {
	session, err := p11.getSession()
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to get session [%s]", err)
	}
	defer p11.returnSession(err, session)

	keyHandle, err := p11.ctx.GenerateKey(session, []*pkcs11.Mechanism{mech}, attrs)
	if err != nil {
		return nil, err
	}
	return &keyHandle, nil
}

// ExportRSAPublicKey export a rsa public key of pkcs11 rsa private key
func (p11 *P11Handle) ExportRSAPublicKey(id []byte) (*rsa.PublicKey, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_PUBLIC_EXPONENT, nil),
		pkcs11.NewAttribute(pkcs11.CKA_MODULUS, nil),
	}
	attrs, err := p11.getAttributes(id, template)
	if err != nil {
		return nil, err
	}
	n, e := big.NewInt(0), int(0)
	for _, a := range attrs {
		if a.Type == pkcs11.CKA_MODULUS {
			n.SetBytes(a.Value)
		} else if a.Type == pkcs11.CKA_PUBLIC_EXPONENT {
			bigE := big.NewInt(0)
			bigE.SetBytes(a.Value)
			e = int(bigE.Int64())
		}
	}
	if e == 0 || n.Cmp(big.NewInt(0)) == 0 {
		return nil, errors.New("public key missing either modulus or exponent")
	}

	return &rsa.PublicKey{
		N: n,
		E: e,
	}, nil
}

// ExportECDSAPublicKey export a ecdsa/sm2 public key of pkcs11 ecdsa/sm2 private key
func (p11 *P11Handle) ExportECDSAPublicKey(id []byte, keyType P11KeyType) (interface{}, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_EC_PARAMS, nil),
		pkcs11.NewAttribute(pkcs11.CKA_EC_POINT, nil),
		//pkcs11.NewAttribute(pkcs11.CKA_LABEL, nil),
		//pkcs11.NewAttribute(pkcs11.CKA_PUBLIC_KEY_INFO, nil), //PKCS#11 specification v2.40 support!
	}
	attrs, err := p11.getAttributes(id, template)
	if err != nil {
		return nil, err
	}
	if len(attrs) < 2 {
		return nil, errors.New("Got attribute not enough, should greater than 2")
	}

	var curve elliptic.Curve
	if keyType == SM2 {
		curve = sm2.P256Sm2()
	} else {
		curve, err = unmarshalEcParams(attrs[0].Value)
		if err != nil {
			return nil, err
		}
	}

	x, y, err := unmarshalEcPoint(curve, attrs[1].Value)
	if err != nil {
		return nil, err
	}

	if keyType == SM2 {
		return &sm2.PublicKey{Curve: curve, X: x, Y: y}, nil
	}
	return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
}

// getSecretKeySize returns a pkcs11 secret key length
func (p11 *P11Handle) getSecretKeySize(obj pkcs11.ObjectHandle) (int, error) {
	session, err := p11.getSession()
	if err != nil {
		return 0, errors.WithMessage(err, "failed to get pkcs11 session")
	}
	defer p11.returnSession(err, session)

	//CKA_VALUE_LEN
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_VALUE_LEN, nil),
	}
	attrs, err := p11.ctx.GetAttributeValue(session, obj, template)
	if err != nil {
		return 0, errors.WithMessage(err, "failed to get aes key CKA_VALUE_LEN")
	}
	if len(attrs) == 1 {
		return bytesToInt(attrs[0].Value)
	}

	//CKA_VALUE
	template = []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_VALUE, nil),
	}
	attrs, err = p11.ctx.GetAttributeValue(session, obj, template)
	if err != nil {
		return 0, errors.WithMessage(err, "failed to get aes key attribute")
	}
	if attrs == nil || len(attrs) < 1 {
		return 0, errors.New("attributes is empty")
	}
	return len(attrs[0].Value), nil
}

// bytesToInt le bytes to int32, little endian
func bytesToInt(b []byte) (int, error) {
	bytesBuffer := bytes.NewBuffer(b)

	var x uint32
	err := binary.Read(bytesBuffer, binary.LittleEndian, &x)
	if err != nil {
		return -1, err
	}
	return int(x), nil
}
