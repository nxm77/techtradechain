/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"crypto/rand"
	"fmt"
	"strconv"

	"techtradechain.com/techtradechain/common/v2/crypto/hsm"

	"techtradechain.com/techtradechain/common/v2/crypto/sym/util"

	"github.com/pkg/errors"

	"techtradechain.com/techtradechain/common/v2/crypto/sym/modes"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"github.com/miekg/pkcs11"
)

var defaultAESOpts = &bccrypto.EncOpts{
	EncodingType: modes.PADDING_PKCS5,
	BlockMode:    modes.BLOCK_MODE_CBC,
	EnableMAC:    true,
	Hash:         0,
	Label:        nil,
	EnableASN1:   true,
}

var _ bccrypto.SymmetricKey = (*aesKey)(nil)

type aesKey struct {
	p11Ctx    *P11Handle
	keyId     []byte
	keyType   P11KeyType
	keyObject pkcs11.ObjectHandle
	keySize   int
	blockSize int
}

// NewAESKey returns a pkcs11 aes key whose type is bccrypto.SymmetricKey
func NewAESKey(ctx *P11Handle, keyId []byte) (bccrypto.SymmetricKey, error) {
	//find private key
	id, err := strconv.Atoi(string(keyId))
	if err != nil {
		return nil, err
	}
	keyIdStr, err := hsm.GetHSMAdapter("").PKCS11_GetAESKeyId(id)
	if err != nil {
		return nil, err
	}

	obj, err := ctx.findSecretKey([]byte(keyIdStr))
	if err != nil {
		return nil, errors.WithMessagef(err, "PKCS11 error: fail to find aes key, keyId = %s", keyIdStr)
	}

	sk := aesKey{p11Ctx: ctx,
		keyId:     keyId,
		keyObject: *obj,
		keyType:   AES,
		blockSize: 16,
	}

	sk.keySize, err = ctx.getSecretKeySize(*obj)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get aes keySize")
	}
	return &sk, nil
}

func (s *aesKey) Bytes() ([]byte, error) {
	return s.keyId, nil
}

func (s *aesKey) Type() bccrypto.KeyType {
	return bccrypto.AES
}

func (s *aesKey) String() (string, error) {
	return string(s.keyId), nil
}

func (s *aesKey) Encrypt(plain []byte) ([]byte, error) {
	return s.EncryptWithOpts(plain, defaultAESOpts)
}

func (s *aesKey) EncryptWithOpts(plain []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	iv := make([]byte, s.blockSize)
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}
	var cipherWithPad []byte
	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			plainWithPad := util.PKCS5Padding(plain, s.blockSize)
			ciphertex, err := s.p11Ctx.Encrypt(s.keyObject, pkcs11.NewMechanism(pkcs11.CKM_AES_CBC, iv), plainWithPad)
			if err != nil {
				return nil, err
			}
			cipherWithPad = append(iv, ciphertex...)
		default:
			return nil, fmt.Errorf("sm4 CBC encryption fails: invalid padding scheme [%s]", opts.EncodingType)
		}
	default:
		return nil, fmt.Errorf("sm4 encryption fails: unknown cipher block mode [%s]", opts.BlockMode)
	}

	return cipherWithPad, nil
}

func (s *aesKey) Decrypt(ciphertext []byte) ([]byte, error) {
	return s.DecryptWithOpts(ciphertext, defaultAESOpts)
}

func (s *aesKey) DecryptWithOpts(ciphertext []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	if len(ciphertext) < s.blockSize {
		return nil, errors.New("invalid ciphertext length")
	}
	var plain []byte
	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			iv := ciphertext[:s.blockSize]
			out, err := s.p11Ctx.Decrypt(s.keyObject, pkcs11.NewMechanism(pkcs11.CKM_AES_CBC, iv), ciphertext[s.blockSize:])
			if err != nil {
				return nil, fmt.Errorf("PKCS11 error: fail to encrypt [%s]", err)
			}
			plain, err = util.PKCS5UnPadding(out)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("sm4 CBC encryption fails: invalid padding scheme [%s]", opts.EncodingType)
		}
	default:
		return nil, fmt.Errorf("sm4 encryption fails: unknown cipher block mode [%s]", opts.BlockMode)
	}

	return plain, nil
}
