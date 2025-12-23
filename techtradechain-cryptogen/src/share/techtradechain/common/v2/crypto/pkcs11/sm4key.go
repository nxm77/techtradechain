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

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/modes"
	"github.com/miekg/pkcs11"
	"github.com/pkg/errors"
)

var defaultSM4Opts = &bccrypto.EncOpts{
	EncodingType: modes.PADDING_PKCS5,
	BlockMode:    modes.BLOCK_MODE_CBC,
	EnableMAC:    true,
	Hash:         0,
	Label:        nil,
	EnableASN1:   false,
}

var _ bccrypto.SymmetricKey = (*sm4Key)(nil)

type sm4Key struct {
	p11Ctx    *P11Handle
	keyId     []byte
	keyType   P11KeyType
	keyObject pkcs11.ObjectHandle
	blockSize int
}

// NewSM4Key return a pkcs11 sm4 key
func NewSM4Key(ctx *P11Handle, keyId []byte) (bccrypto.SymmetricKey, error) {
	//find private key
	id, err := strconv.Atoi(string(keyId))
	if err != nil {
		return nil, err
	}
	keyIdStr, err := hsm.GetHSMAdapter("").PKCS11_GetSM4KeyId(id)
	if err != nil {
		return nil, err
	}
	obj, err := ctx.findSecretKey([]byte(keyIdStr))
	if err != nil {
		return nil, fmt.Errorf("PKCS11 error: fail to find sm4 key [%s]", err)
	}

	return &sm4Key{
		p11Ctx:    ctx,
		keyId:     keyId,
		keyObject: *obj,
		keyType:   SM4,
		blockSize: 16,
	}, nil
}

func (s *sm4Key) Bytes() ([]byte, error) {
	return s.keyId, nil
}

func (s *sm4Key) Type() bccrypto.KeyType {
	return bccrypto.SM4
}

func (s *sm4Key) String() (string, error) {
	return string(s.keyId), nil
}

func (s *sm4Key) Encrypt(plain []byte) ([]byte, error) {
	return s.EncryptWithOpts(plain, defaultSM4Opts)
}

func (s *sm4Key) EncryptWithOpts(plain []byte, opts *bccrypto.EncOpts) ([]byte, error) {
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
			ciphertex, err := s.p11Ctx.Encrypt(s.keyObject, pkcs11.NewMechanism(CKM_SM4_CBC, iv), plainWithPad)
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

func (s *sm4Key) Decrypt(ciphertext []byte) ([]byte, error) {
	return s.DecryptWithOpts(ciphertext, defaultSM4Opts)
}

func (s *sm4Key) DecryptWithOpts(ciphertext []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	if len(ciphertext) < s.blockSize {
		return nil, errors.New("invalid ciphertext length")
	}
	var plain []byte
	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			iv := ciphertext[:s.blockSize]
			out, err := s.p11Ctx.Decrypt(s.keyObject, pkcs11.NewMechanism(CKM_SM4_CBC, iv), ciphertext[s.blockSize:])
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
