/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package sm4

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/modes"
	"techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/tencentsm"
)

var defaultSM4Opts = &crypto.EncOpts{
	EncodingType: modes.PADDING_PKCS5,
	BlockMode:    modes.BLOCK_MODE_CBC,
	EnableMAC:    true,
	Hash:         0,
	Label:        nil,
	EnableASN1:   false,
}

var _ crypto.SymmetricKey = (*SM4Key)(nil)

type SM4Key struct {
	Key []byte
}

func (s SM4Key) Bytes() ([]byte, error) {
	return s.Key, nil
}

func (s SM4Key) Type() crypto.KeyType {
	return crypto.SM4
}

func (s SM4Key) String() (string, error) {
	return hex.EncodeToString(s.Key), nil
}

func (s SM4Key) Encrypt(plain []byte) ([]byte, error) {
	return s.EncryptWithOpts(plain, defaultSM4Opts)
}

func (s SM4Key) EncryptWithOpts(plain []byte, opts *crypto.EncOpts) ([]byte, error) {
	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:
		var out = make([]byte, len(plain)+utils.SM4_BlockSize)
		var outLen int
		var iv [16]byte
		if _, err := rand.Read(iv[:]); err != nil {
			return nil, err
		}
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			tencentsm.SM4_CBC_Encrypt(plain, len(plain), out[:], &outLen, s.Key, iv[:])
			return append(iv[:], out[:outLen]...), nil
		default:
			return nil, fmt.Errorf("SM4 encryption fails, unknown encoding type [%s]", opts.EncodingType)
		}
	default:
		return nil, fmt.Errorf("SM4 encryption fails: unknown cipher block mode [%s]", opts.BlockMode)
	}
}

func (s SM4Key) Decrypt(ciphertext []byte) ([]byte, error) {
	return s.DecryptWithOpts(ciphertext, defaultSM4Opts)
}

// DecryptWithOpts decrypt ciphertext according to opts
func (s SM4Key) DecryptWithOpts(ciphertext []byte, opts *crypto.EncOpts) ([]byte, error) {
	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:
		var out = make([]byte, len(ciphertext))
		var outLen int
		if len(ciphertext) < utils.SM4_BlockSize {
			return nil, fmt.Errorf("invalid ciphertext length, want > 16, got %d", len(ciphertext))
		}
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			tencentsm.SM4_CBC_Decrypt(ciphertext[utils.SM4_BlockSize:], len(ciphertext)-utils.SM4_BlockSize,
				out[:], &outLen, s.Key, ciphertext[:utils.SM4_BlockSize])
			return out[:outLen], nil
		default:
			return nil, fmt.Errorf("SM4 encryption fails, unknown encoding type [%s]", opts.EncodingType)
		}
	default:
		return nil, fmt.Errorf("SM4 encryption fails: unknown cipher block mode [%s]", opts.BlockMode)
	}
}
