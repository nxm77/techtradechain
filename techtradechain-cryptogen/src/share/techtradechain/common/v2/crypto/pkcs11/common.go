/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package pkcs11

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/asn1"
	"fmt"
	"math/big"
	"strings"
	"sync/atomic"

	"github.com/miekg/pkcs11"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	bcecdsa "techtradechain.com/techtradechain/common/v2/crypto/asym/ecdsa"
	bcrsa "techtradechain.com/techtradechain/common/v2/crypto/asym/rsa"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"github.com/pkg/errors"
	"github.com/tjfoc/gmsm/sm2"
)

// P11KeyType defines the pkcs11 key type
type P11KeyType string

const (
	// pkcs11 key types
	RSA   P11KeyType = "RSA"
	ECDSA P11KeyType = "ECDSA"
	SM2   P11KeyType = "SM2"

	AES P11KeyType = "AES"
	SM4 P11KeyType = "SM4"

	UNKNOWN P11KeyType = "UNKNOWN"
)

// GenOpts is used for key generate
type GenOpts struct {
	KeyBits     int
	CurveParams string
}

// rsaPublicKey reflects the ASN.1 structure of a PKCS#1 public key.
type rsaPublicKeyASN struct {
	N *big.Int
	E int
}

// ecdsaSignature is a r-s style ecdsa signature
//type ecdsaSignature struct {
//	R, S *big.Int
//}

// GetPublicKeySKI returns the pk's ski
func (p11 *P11Handle) GetPublicKeySKI(pk bccrypto.PublicKey) ([]byte, error) {
	if pk == nil {
		return nil, fmt.Errorf("public key is nil")
	}

	var pkBytes []byte
	var err error
	switch pk.(type) {
	case *bcecdsa.PublicKey:
		pubKey := pk.ToStandardKey()
		switch k := pubKey.(type) {
		case *ecdsa.PublicKey:
			pkBytes = elliptic.Marshal(k.Curve, k.X, k.Y)
		case *sm2.PublicKey:
			pkBytes = elliptic.Marshal(k.Curve, k.X, k.Y)
		default:
			return nil, errors.New("unknown public key type")
		}
	case *bcrsa.PublicKey:
		k, _ := pk.ToStandardKey().(*rsa.PublicKey)
		pkBytes, err = asn1.Marshal(rsaPublicKeyASN{
			N: k.N,
			E: k.E,
		})
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown public key type")
	}

	return hash.GetByStrType(p11.hash, pkBytes)
}

//NewPrivateKey returns a crypto PrivateKey which wraps a pkcs11 private key
func NewPrivateKey(p11 *P11Handle, keyId string, keyType bccrypto.KeyType) (bccrypto.PrivateKey, error) {
	kType := convertToP11KeyType(keyType)
	var privKey bccrypto.PrivateKey
	var err error
	switch kType {
	case RSA:
		privKey, err = NewP11RSAPrivateKey(p11, []byte(keyId), kType)
	case ECDSA, SM2:
		privKey, err = NewP11ECDSAPrivateKey(p11, []byte(keyId), kType)
	default:
		return nil, errors.New("KeyType is UNKNOWN")
	}
	return privKey, err
}

// nolint
var nextId uint32

// nolint
func incNextId() uint32 {
	return atomic.AddUint32(&nextId, 1)
}

//convertToP11KeyType convert KeyType to internal P11KeyType
func convertToP11KeyType(keyType bccrypto.KeyType) P11KeyType {
	keyTypeStr := bccrypto.KeyType2NameMap[keyType]
	if strings.Contains(keyTypeStr, "RSA") {
		return RSA
	} else if strings.Contains(keyTypeStr, "SM2") {
		return SM2
	} else if strings.Contains(keyTypeStr, "ECC") || strings.Contains(keyTypeStr, "ECDSA") {
		return ECDSA
	} else if strings.Contains(keyTypeStr, "AES") {
		return AES
	} else if strings.Contains(keyTypeStr, "SM4") {
		return SM4
	}

	return UNKNOWN
}

//NewSecretKey returns a crypto SymmetricKey which wraps a pkcs11 secret key
func NewSecretKey(p11 *P11Handle, keyId string, keyType bccrypto.KeyType) (bccrypto.SymmetricKey, error) {
	if p11 == nil || len(keyId) == 0 {
		return nil, errors.New("Invalid parameter, p11 or keyId is nil")
	}

	kType := convertToP11KeyType(keyType)
	switch kType {
	case AES:
		return NewAESKey(p11, []byte(keyId))
	case SM4:
		return NewSM4Key(p11, []byte(keyId))
	default:
		return nil, errors.New("KeyType is UNKNOWN")
	}
}

//GenSecretKey generate a new pkcs11 secret key
func GenSecretKey(p11 *P11Handle, keyId string, keyType bccrypto.KeyType, keySize int) (bccrypto.SymmetricKey, error) {
	if p11 == nil || len(keyId) == 0 {
		return nil, errors.New("Invalid parameter, p11 or keyId is nil")
	}
	kType := convertToP11KeyType(keyType)
	switch kType {
	case AES:
		if keySize != 16 && keySize != 24 && keySize != 32 {
			return nil, fmt.Errorf("invalid aes keySize, want 16|24|32, got %d", keySize)
		}
		keyTemplate := []*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, false),
			pkcs11.NewAttribute(pkcs11.CKA_ENCRYPT, true),
			pkcs11.NewAttribute(pkcs11.CKA_DECRYPT, true),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, []byte(keyId)),
			pkcs11.NewAttribute(pkcs11.CKA_VALUE_LEN, keySize),
		}

		mech := pkcs11.NewMechanism(pkcs11.CKM_AES_KEY_GEN, nil)

		_, err := p11.GenerateKey(mech, keyTemplate)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to generate pkcs11 aes key")
		}
		return NewAESKey(p11, []byte(keyId))
	case SM4:
		keyTemplate := []*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, false),
			pkcs11.NewAttribute(pkcs11.CKA_PRIVATE, true),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, []byte(keyId)),
			pkcs11.NewAttribute(pkcs11.CKA_ENCRYPT, true),
		}
		mech := pkcs11.NewMechanism(CKM_SM4_KEY_GEN, nil)
		_, err := p11.GenerateKey(mech, keyTemplate)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to generate pkcs11 sm4 key")
		}
		return NewSM4Key(p11, []byte(keyId))
	default:
		return nil, errors.New("KeyType is UNKNOWN")
	}
}

// GenKeyPair is used to generate a pkcs11 private key whose type is bccrypto.PrivateKey
func GenKeyPair(p11 *P11Handle, keyId string, keyType bccrypto.KeyType, opts *GenOpts) (bccrypto.PrivateKey, error) {
	if p11 == nil || len(keyId) == 0 {
		return nil, errors.New("Invalid parameter, p11 or keyId is nil")
	}
	kType := convertToP11KeyType(keyType)
	switch kType {
	case SM2:
		publicKeyTemplate := []*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_KEY_TYPE, CKK_SM2),
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, false),
			pkcs11.NewAttribute(pkcs11.CKA_VERIFY, true),
			pkcs11.NewAttribute(pkcs11.CKA_ENCRYPT, true),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, []byte(keyId)),
		}
		privateKeyTemplate := []*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, false),
			pkcs11.NewAttribute(pkcs11.CKA_SIGN, true),
			pkcs11.NewAttribute(pkcs11.CKA_DECRYPT, true),
			pkcs11.NewAttribute(pkcs11.CKA_PRIVATE, true),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, []byte(keyId)),
			pkcs11.NewAttribute(pkcs11.CKA_SENSITIVE, true),
		}
		mech := pkcs11.NewMechanism(CKM_SM2_KEY_PAIR_GEN, nil)
		_, _, err := p11.GenKeyPair(mech, privateKeyTemplate, publicKeyTemplate)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to generate pkcs11 sm2 key")
		}
		return NewP11ECDSAPrivateKey(p11, []byte(keyId), kType)
	case ECDSA:
		publicKeyTemplate := []*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_KEY_TYPE, pkcs11.CKK_EC),
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, false),
			pkcs11.NewAttribute(pkcs11.CKA_VERIFY, true),
			pkcs11.NewAttribute(pkcs11.CKA_ENCRYPT, true),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, []byte(keyId)),
			pkcs11.NewAttribute(pkcs11.CKA_ECDSA_PARAMS, opts.CurveParams),
		}
		privateKeyTemplate := []*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_KEY_TYPE, pkcs11.CKK_EC),
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, false),
			pkcs11.NewAttribute(pkcs11.CKA_SIGN, true),
			pkcs11.NewAttribute(pkcs11.CKA_DECRYPT, true),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, []byte(keyId)),
			pkcs11.NewAttribute(pkcs11.CKA_ECDSA_PARAMS, opts.CurveParams),
		}
		mech := pkcs11.NewMechanism(pkcs11.CKM_EC_KEY_PAIR_GEN, nil)
		_, _, err := p11.GenKeyPair(mech, privateKeyTemplate, publicKeyTemplate)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to generate pkcs11 ecdsa key")
		}
		return NewP11ECDSAPrivateKey(p11, []byte(keyId), kType)
	case RSA:
		if opts.KeyBits != 1024 && opts.KeyBits != 2048 && opts.KeyBits != 3072 {
			return nil, errors.New("Invalid rsa keyBits, Must be 1024|2048|3072")
		}
		publicKeyTemplate := []*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_KEY_TYPE, pkcs11.CKK_RSA),
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, false),
			pkcs11.NewAttribute(pkcs11.CKA_VERIFY, true),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, []byte(keyId)),
			pkcs11.NewAttribute(pkcs11.CKA_MODULUS_BITS, opts.KeyBits),
		}
		privateKeyTemplate := []*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, false),
			pkcs11.NewAttribute(pkcs11.CKA_SIGN, true),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, []byte(keyId)),
			pkcs11.NewAttribute(pkcs11.CKA_SENSITIVE, true),
		}
		mech := pkcs11.NewMechanism(pkcs11.CKM_RSA_PKCS_KEY_PAIR_GEN, nil)
		_, _, err := p11.GenKeyPair(mech, privateKeyTemplate, publicKeyTemplate)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to generate pkcs11 rsa key")
		}
		return NewP11RSAPrivateKey(p11, []byte(keyId), kType)
	default:
		return nil, errors.New("unknown key type")
	}
}
