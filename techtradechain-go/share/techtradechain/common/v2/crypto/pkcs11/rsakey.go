/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"crypto"
	"crypto/rsa"
	"fmt"
	"io"
	"strconv"

	"techtradechain.com/techtradechain/common/v2/crypto/hsm"

	bcrsa "techtradechain.com/techtradechain/common/v2/crypto/asym/rsa"

	"github.com/pkg/errors"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"github.com/miekg/pkcs11"
)

type rsaPrivateKey struct {
	priv *p11RsaPrivateKey
}

func (r rsaPrivateKey) Public() crypto.PublicKey {
	return r.priv.pubKey.ToStandardKey()
}

func (r rsaPrivateKey) Sign(rand io.Reader, digest []byte, opts crypto.SignerOpts) (signature []byte, err error) {
	if opts == nil {
		return r.priv.Sign(digest)
	}
	if _, ok := opts.(*rsa.PSSOptions); ok {
		return r.priv.SignWithOpts(digest, &bccrypto.SignOpts{
			EncodingType: bcrsa.RSA_PSS,
			Hash:         bccrypto.HashType(opts.HashFunc())})
	}
	return r.priv.SignWithOpts(digest, &bccrypto.SignOpts{Hash: bccrypto.HashType(opts.HashFunc())})
}

// p11RsaPrivateKey represents pkcs11 rsa private key
type p11RsaPrivateKey struct {
	p11Ctx    *P11Handle
	pubKey    bccrypto.PublicKey
	keyId     []byte
	keyType   P11KeyType
	keyObject pkcs11.ObjectHandle

	signer crypto.Signer
}

// NewP11RSAPrivateKey returns a pkcs11 rsa key
func NewP11RSAPrivateKey(p11 *P11Handle, keyId []byte, keyType P11KeyType) (bccrypto.PrivateKey, error) {
	if p11 == nil || keyId == nil {
		return nil, errors.New("Invalid parameter, p11 or keyId is nil")
	}

	//find private key
	id, err := strconv.Atoi(string(keyId))
	if err != nil {
		return nil, err
	}
	keyIdStr, err := hsm.GetHSMAdapter("").PKCS11_GetRSAKeyId(id, true)
	if err != nil {
		return nil, err
	}
	obj, err := p11.findPrivateKey([]byte(keyIdStr))
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to find private key, keyId = %s", keyIdStr)
	}
	//export public key
	keyIdStr, err = hsm.GetHSMAdapter("").PKCS11_GetRSAKeyId(id, false)
	if err != nil {
		return nil, err
	}
	pubKey, err := p11.ExportRSAPublicKey([]byte(keyIdStr))
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to export rsa public key, keyId = %s", keyIdStr)
	}

	p11PrivateKey := &p11RsaPrivateKey{
		p11Ctx:    p11,
		pubKey:    &bcrsa.PublicKey{K: pubKey},
		keyId:     keyId,
		keyType:   keyType,
		keyObject: *obj,
	}

	p11PrivateKey.signer = &rsaPrivateKey{p11PrivateKey}

	return p11PrivateKey, nil
}

func (sk *p11RsaPrivateKey) Type() bccrypto.KeyType {
	return sk.PublicKey().Type()
}

func (sk *p11RsaPrivateKey) Bytes() ([]byte, error) {
	return sk.keyId, nil
}

func (sk *p11RsaPrivateKey) String() (string, error) {
	return string(sk.keyId), nil
}

func (sk *p11RsaPrivateKey) PublicKey() bccrypto.PublicKey {
	return sk.pubKey
}

func (sk *p11RsaPrivateKey) Sign(data []byte) ([]byte, error) {
	mech := uint(pkcs11.CKM_SHA256_RSA_PKCS)
	sig, err := sk.p11Ctx.Sign(sk.keyObject, pkcs11.NewMechanism(mech, nil), data)
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to sign [%s]", err)
	}

	return sig, nil
}

func (sk *p11RsaPrivateKey) SignWithOpts(msg []byte, opts *bccrypto.SignOpts) ([]byte, error) {
	if opts == nil {
		return sk.Sign(msg)
	}
	var mech uint

	switch opts.EncodingType {
	case bcrsa.RSA_PSS:
		//mech = pkcs11.CKM_SHA256_RSA_PKCS_PSS
		return nil, errors.New("rsa_pss not supported, todo")
	default:
		switch opts.Hash {
		case bccrypto.HASH_TYPE_SHA256:
			mech = pkcs11.CKM_SHA256_RSA_PKCS
		case bccrypto.HASH_TYPE_SHA3_256:
			mech = pkcs11.CKM_SHA3_256_RSA_PKCS
		default:
			return nil, fmt.Errorf("PKCS11 error: unsupported hash type [%v]", opts.Hash)
		}
	}

	sig, err := sk.p11Ctx.Sign(sk.keyObject, pkcs11.NewMechanism(mech, nil), msg)
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to sign [%s]", err)
	}

	return sig, nil
}

func (sk *p11RsaPrivateKey) ToStandardKey() crypto.PrivateKey {
	return sk.signer
}
