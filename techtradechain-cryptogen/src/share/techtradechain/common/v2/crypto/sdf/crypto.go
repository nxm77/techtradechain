/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sdf

import (
	"encoding/asn1"
	"fmt"
	"math/big"

	"github.com/pkg/errors"

	"techtradechain.com/techtradechain/common/v2/crypto/hsm"

	"techtradechain.com/techtradechain/common/v2/crypto/sdf/base"
	"github.com/tjfoc/gmsm/sm2"
)

// SDFKey represents a sdf key
type SDFKey struct {
	KeyId   uint
	KeyPwd  []byte
	KeyType SDFKeyType
}

// GenerateRandom return a random bytes with fixed length
func (h *SDFHandle) GenerateRandom(length int) ([]byte, error) {
	session, err := h.getSession()
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	return h.ctx.SDFGenerateRandom(session, uint(length))
}

// ECCInternalSign signs the input with a given mechanism.
func (h *SDFHandle) ECCInternalSign(key SDFKey, msg []byte) ([]byte, error) {
	session, err := h.getSession()
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	{
		/*
			check pwd
			this depends on HSM vendors :-(
		*/
		accessKeyId, need := hsm.GetHSMAdapter("").SDF_GetSM2KeyAccessRight(int(key.KeyId))
		if need {
			err = h.ctx.SDFGetPrivateKeyAccessRight(session, uint(accessKeyId), key.KeyPwd, uint(len(key.KeyPwd)))
			if err != nil {
				return nil, errors.WithMessage(err, "failed to SDFGetPrivateKeyAccessRight before sign")
			}
		}
	}

	sign, err := h.ctx.SDFInternalSign_ECC(session, key.KeyId, msg, uint(len(msg)))
	if err != nil {
		return nil, errors.WithMessage(err, "failed to SDFInternalSign_ECC")
	}
	r := big.NewInt(0).SetBytes([]byte(sign.R))
	s := big.NewInt(0).SetBytes([]byte(sign.S))

	return asn1.Marshal(ECCSignature{r, s})
}

// Verify verifies a signature over a message with a given mechanism.
func (h *SDFHandle) Verify(key SDFKey, msg []byte, sig base.ECCSignature) error {
	session, err := h.getSession()
	if err != nil {
		return fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	//err = h.ctx.SDFGetPrivateKeyAccessRight(session, key.KeyId, key.KeyPwd, uint(len(key.KeyPwd)))
	//if err != nil {
	//	return err
	//}

	err = h.ctx.SDFInternalVerify_ECC(session, key.KeyId, msg, uint(len(msg)), sig)
	if err != nil {
		return err
	}
	return nil
}

// Encrypt encrypts a plaintext with a given mechanism.
func (h *SDFHandle) Encrypt(key SDFKey, plain []byte) ([]byte, error) {
	session, err := h.getSession()
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	{
		/*
			check pwd
			this depends on HSM vendors :-(
		*/
		accessKeyId, need := hsm.GetHSMAdapter("").SDF_GetSM2KeyAccessRight(int(key.KeyId))
		if need {
			err = h.ctx.SDFGetPrivateKeyAccessRight(session, uint(accessKeyId), key.KeyPwd, uint(len(key.KeyPwd)))
			if err != nil {
				return nil, err
			}
		}
	}

	out, err := h.ctx.SDFInternalEncrypt_ECC(session, key.KeyId, base.SGD_SM2_3, plain, uint(len(plain)))
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to execute sm2 encrypt")
	}
	return asn1.Marshal(out)
}

// Decrypt decrypt a cipher with a given mechanism.
func (h *SDFHandle) Decrypt(key SDFKey, cipher []byte) ([]byte, error) {
	session, err := h.getSession()
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	{
		/*
			check pwd
			this depends on HSM vendors :-(
		*/
		accessKeyId, need := hsm.GetHSMAdapter("").SDF_GetSM2KeyAccessRight(int(key.KeyId))
		if need {
			err = h.ctx.SDFGetPrivateKeyAccessRight(session, uint(accessKeyId), key.KeyPwd, uint(len(key.KeyPwd)))
			if err != nil {
				return nil, err
			}
		}
	}

	//out, _, err := h.ctx.SDFInternalDecrypt_ECC(session, key.KeyId, sdf.SGD_SM2_3, cipher)
	//if err != nil {
	//	return nil, err
	//}
	//_ = out
	return nil, nil
}

// GenKeyPair returns asym keypair
func (h *SDFHandle) GenKeyPair() (pri *base.ECCrefPrivateKey, pub *base.ECCrefPublicKey, err error) {
	session, err := h.getSession()
	if err != nil {
		return nil, nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	pubHandle, privHandle, err := h.ctx.SDFGenerateKeyPair_ECC(session, base.SGD_SM2, 256)
	if err != nil {
		return nil, nil, err
	}
	return &privHandle, &pubHandle, nil
}

// GenerateKey returns sym key
func (h *SDFHandle) GenerateKey(length int) ([]byte, error) {
	session, err := h.getSession()
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	randomBytes, err := h.ctx.SDFGenerateRandom(session, uint(length))
	if err != nil {
		return nil, err
	}
	return randomBytes, nil
}

// ExportECDSAPublicKey export a ecdsa/sm2 public key of pkcs11 ecdsa/sm2 private key
func (h *SDFHandle) ExportECDSAPublicKey(key SDFKey) (interface{}, error) {
	session, err := h.getSession()
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	pub, err := h.ctx.SDFExportSignPublicKey_ECC(session, key.KeyId)
	if err != nil {
		return nil, err
	}

	x, y := big.NewInt(0), big.NewInt(0)
	x.SetBytes([]byte(pub.X))
	y.SetBytes([]byte(pub.Y))

	sm2PubKey := &sm2.PublicKey{
		Curve: sm2.P256Sm2(),
		X:     x,
		Y:     y,
	}

	return sm2PubKey, err
}

// SymEncrypt encrypts a plaintext with a given mechanism.
func (h *SDFHandle) SymEncrypt(keyHandle base.SessionHandle, mode uint, iv []byte, plain []byte) ([]byte, error) {
	session, err := h.getSession()
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	iv2 := make([]byte, len(iv))
	copy(iv2, iv)
	encData, encDataLength, err := h.ctx.SDFEncrypt(session, keyHandle, mode, iv2, plain, uint(len(plain)))
	if err != nil {
		return nil, errors.WithMessage(err, "failed to encrypt")
	}
	return encData[:encDataLength], nil
}

// SymDecrypt decrypt a cipher with a given mechanism.
func (h *SDFHandle) SymDecrypt(keyHandle base.SessionHandle, mode uint, iv []byte, cipher []byte) ([]byte, error) {
	session, err := h.getSession()
	if err != nil {
		return nil, fmt.Errorf("[SDF] error: fail to get session [%s]", err)
	}
	defer h.returnSession(err, session)

	iv2 := make([]byte, len(iv))
	copy(iv2, iv)
	decdata, decdataLength, err := h.ctx.SDFDecrypt(session, keyHandle, mode, iv2, cipher, uint(len(cipher)))
	if err != nil {
		return nil, errors.WithMessage(err, "failed to decrypt")
	}
	return decdata[:decdataLength], nil
}
