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

	"techtradechain.com/techtradechain/common/v2/opencrypto/gmssl/gmssl"

	"techtradechain.com/techtradechain/common/v2/opencrypto/utils"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/modes"
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
		var iv [16]byte
		if _, err := rand.Read(iv[:]); err != nil {
			return nil, err
		}
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			encrypter, err := gmssl.NewCipherContext("SMS4", s.Key, iv[:], true)
			if err != nil {
				return nil, err
			}
			cipher1, err := encrypter.Update(plain)
			if err != nil {
				return nil, err
			}
			cipher2, err := encrypter.Final()
			if err != nil {
				return nil, err
			}
			ciphertext := make([]byte, 0, len(cipher1)+len(cipher2))
			ciphertext = append(append(ciphertext, cipher1...), cipher2...)
			return append(iv[:], ciphertext...), nil
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

func (s SM4Key) DecryptWithOpts(ciphertext []byte, opts *crypto.EncOpts) ([]byte, error) {
	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:
		if len(ciphertext) < utils.SM4_BlockSize {
			return nil, fmt.Errorf("invalid ciphertext length, want > 16, got %d", len(ciphertext))
		}
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			decrypter, err := gmssl.NewCipherContext("SMS4", s.Key, ciphertext[:utils.SM4_BlockSize], false)
			if err != nil {
				return nil, err
			}
			plain1, err := decrypter.Update(ciphertext[utils.SM4_BlockSize:])
			if err != nil {
				return nil, err
			}
			plain2, err := decrypter.Final()
			if err != nil {
				return nil, err
			}
			plain := make([]byte, 0, len(plain1)+len(plain2))
			plain = append(append(plain, plain1...), plain2...)
			return plain, nil
		default:
			return nil, fmt.Errorf("SM4 encryption fails, unknown encoding type [%s]", opts.EncodingType)
		}
	default:
		return nil, fmt.Errorf("SM4 encryption fails: unknown cipher block mode [%s]", opts.BlockMode)
	}
}
