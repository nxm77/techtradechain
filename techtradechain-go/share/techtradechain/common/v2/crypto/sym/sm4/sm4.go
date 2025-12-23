/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package sm4

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/modes"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/util"
	"github.com/tjfoc/gmsm/sm4"
)

var defaultSM4Opts = &crypto.EncOpts{
	EncodingType: modes.PADDING_NONE,
	BlockMode:    modes.BLOCK_MODE_GCM,
	EnableMAC:    true,
	Hash:         0,
	Label:        nil,
	EnableASN1:   false,
}

type SM4Key struct {
	Key []byte
}

func (sm4Key *SM4Key) Bytes() ([]byte, error) {
	return sm4Key.Key, nil
}

func (sm4Key *SM4Key) String() (string, error) {
	return hex.EncodeToString(sm4Key.Key), nil
}

func (sm4Key *SM4Key) Encrypt(plain []byte) ([]byte, error) {
	return sm4Key.EncryptWithOpts(plain, defaultSM4Opts)
}

func (sm4Key *SM4Key) EncryptWithOpts(plain []byte, opts *crypto.EncOpts) ([]byte, error) {
	// TODO implement different mode
	block, err := sm4.NewCipher(sm4Key.Key)
	if err != nil {
		return nil, err
	}
	msg := util.PKCS5Padding(plain, block.BlockSize())
	iv := make([]byte, block.BlockSize())
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}

	blockMode := cipher.NewCBCEncrypter(block, iv)
	crypted := make([]byte, len(msg)+len(iv))
	blockMode.CryptBlocks(crypted[block.BlockSize():], msg)
	copy(crypted[0:block.BlockSize()], iv)
	return crypted, nil
}

func (sm4Key *SM4Key) Decrypt(crypted []byte) ([]byte, error) {
	return sm4Key.DecryptWithOpts(crypted, defaultSM4Opts)
}

func (sm4Key *SM4Key) DecryptWithOpts(crypted []byte, opts *crypto.EncOpts) ([]byte, error) {
	// TODO implement different mode
	block, err := sm4.NewCipher(sm4Key.Key)
	if err != nil {
		return nil, err
	}

	blockMode := cipher.NewCBCDecrypter(block, crypted[:block.BlockSize()])
	orig := make([]byte, len(crypted)-block.BlockSize())
	blockMode.CryptBlocks(orig, crypted[block.BlockSize():])

	orig, err = util.PKCS5UnPadding(orig)
	if err != nil {
		return nil, err
	}
	return orig, nil
}

func (sm4Key *SM4Key) Type() crypto.KeyType {
	return crypto.SM4
}
