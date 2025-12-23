/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package sdf

import (
	"crypto/rand"
	"fmt"
	"strconv"

	"techtradechain.com/techtradechain/common/v2/crypto/sdf/base"

	"techtradechain.com/techtradechain/common/v2/crypto/sym/util"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/modes"
	"github.com/pkg/errors"
)

const (
	BLOCK_MODE_ECB = "ECB"
	BLOCK_MODE_CTR = "CTR"
)

var defaultSM4Opts = &bccrypto.EncOpts{
	EncodingType: modes.PADDING_PKCS5,
	BlockMode:    BLOCK_MODE_ECB,
	EnableMAC:    true,
	Hash:         0,
	Label:        nil,
	EnableASN1:   false,
}

var _ bccrypto.SymmetricKey = (*sm4Key)(nil)

type sm4Key struct {
	sdfCtx    *SDFHandle
	keyId     uint
	keyPwd    []byte
	keyType   SDFKeyType
	blockSize int

	keyHandle base.SessionHandle
}

func NewSecretKey(sdf *SDFHandle, keyId string, keyPwd []byte, tp bccrypto.KeyType) (bccrypto.SymmetricKey, error) {
	if sdf == nil || len(keyId) == 0 {
		return nil, errors.New("Invalid parameter, sdfHandle or keyId is nil")
	}

	//SM4 or AES
	keyType := convertToSDFKeyType(tp)

	//check keyId
	keyIndex, err := strconv.Atoi(keyId)
	if err != nil {
		return nil, err
	}

	session, err := sdf.getSession()
	if err != nil {
		return nil, err
	}
	defer sdf.returnSession(err, session)

	keyHandle, err := sdf.ctx.SDFGetSymmKeyHandle(session, uint(keyIndex))
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get sym keyHandle, keyIndex = %d", keyIndex)
	}

	return &sm4Key{
		sdfCtx:    sdf,
		keyId:     uint(keyIndex),
		keyPwd:    keyPwd,
		keyType:   keyType,
		blockSize: 16,

		keyHandle: keyHandle,
	}, nil
}

func (s *sm4Key) Bytes() ([]byte, error) {
	return []byte(fmt.Sprintf("%d", s.keyId)), nil
}

func (s *sm4Key) Type() bccrypto.KeyType {
	return bccrypto.SM4
}

func (s *sm4Key) String() (string, error) {
	return fmt.Sprintf("%d", s.keyId), nil
}

func (s *sm4Key) Encrypt(plain []byte) ([]byte, error) {
	return s.EncryptWithOpts(plain, defaultSM4Opts)
}

func (s *sm4Key) EncryptWithOpts(plain []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	if opts == nil {
		opts = defaultSM4Opts
	}
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
			ciphertext, err := s.sdfCtx.SymEncrypt(s.keyHandle, base.SGD_SMS4_CBC, iv, plainWithPad)
			if err != nil {
				return nil, err
			}
			cipherWithPad = append(iv, ciphertext...)
		default:
			return nil, fmt.Errorf("sm4 CBC encryption fails: invalid padding scheme [%s]", opts.EncodingType)
		}
	case BLOCK_MODE_ECB:
		plainWithPad := util.PKCS5Padding(plain, s.blockSize)
		ciphertext, err := s.sdfCtx.SymEncrypt(s.keyHandle, base.SGD_SMS4_ECB, nil, plainWithPad)
		if err != nil {
			return nil, err
		}
		cipherWithPad = ciphertext
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
	if opts == nil {
		opts = defaultSM4Opts
	}
	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			iv := ciphertext[:s.blockSize]
			out, err := s.sdfCtx.SymDecrypt(s.keyHandle, base.SGD_SMS4_CBC, iv, ciphertext[s.blockSize:])
			if err != nil {
				return nil, err
			}
			return util.PKCS5UnPadding(out)
		default:
			return nil, fmt.Errorf("sm4 CBC encryption fails: invalid padding scheme [%s]", opts.EncodingType)
		}
	case BLOCK_MODE_ECB:
		out, err := s.sdfCtx.SymDecrypt(s.keyHandle, base.SGD_SMS4_ECB, nil, ciphertext)
		if err != nil {
			return nil, err
		}
		return util.PKCS5UnPadding(out)
	default:
		return nil, fmt.Errorf("sm4 encryption fails: unknown cipher block mode [%s]", opts.BlockMode)
	}
}
