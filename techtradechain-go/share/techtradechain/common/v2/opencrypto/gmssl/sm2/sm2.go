/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sm2

import (
	"crypto"
	"encoding/pem"
	"io"

	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"

	"techtradechain.com/techtradechain/common/v2/opencrypto/gmssl/gmssl"
	"github.com/pkg/errors"
)

// GenerateKeyPair return a sm2 private key
func GenerateKeyPair() (*PrivateKey, error) {
	sm2keygenargs := &gmssl.PkeyCtxParams{
		Keys:   []string{"ec_paramgen_curve", "ec_param_enc"},
		Values: []string{"sm2p256v1", "named_curve"},
	}
	sk, err := gmssl.GeneratePrivateKey("EC", sm2keygenargs, nil)
	if err != nil {
		return nil, err
	}
	skPem, err := sk.GetUnencryptedPEM()
	if err != nil {
		return nil, err
	}
	p, _ := pem.Decode([]byte(skPem))
	if p == nil {
		return nil, errors.New("invalid private key pem")
	}

	pkPem, err := sk.GetPublicKeyPEM()
	if err != nil {
		return nil, err
	}
	pk, err := gmssl.NewPublicKeyFromPEM(pkPem)
	if err != nil {
		return nil, err
	}
	pubKey := PublicKey{
		PublicKey: pk,
		pkPem:     pkPem,
	}

	return &PrivateKey{PrivateKey: sk, skPem: skPem, Pub: pubKey}, nil
}

type signer struct {
	PrivateKey
}

// Public return the public key
func (s *signer) Public() crypto.PublicKey {
	return s.PublicKey
}

// Sign returns a signature
func (s *signer) Sign(rand io.Reader, msg []byte, opts crypto.SignerOpts) ([]byte, error) {
	return s.PrivateKey.signWithSM3(msg, utils.SM2_DEFAULT_USER_ID)
}
