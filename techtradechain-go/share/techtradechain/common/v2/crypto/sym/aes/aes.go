/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package aes

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/asn1"
	"encoding/hex"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/modes"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/util"
)

const (
	AES_GCM_IV_LENGTH  = 12
	AES_GCM_TAG_LENGTH = 16
)

var defaultAESOpts = &crypto.EncOpts{
	EncodingType: modes.PADDING_NONE,
	BlockMode:    modes.BLOCK_MODE_GCM,
	EnableMAC:    true,
	Hash:         0,
	Label:        nil,
	EnableASN1:   true,
}

type AESKey struct {
	Key []byte
}

type aesCiphertext struct {
	IV         []byte
	Ciphertext []byte
	Tag        []byte
}

func (aesKey *AESKey) Bytes() ([]byte, error) {
	return aesKey.Key, nil
}

func (aesKey *AESKey) String() (string, error) {
	return hex.EncodeToString(aesKey.Key), nil
}

/*
  The ciphertext returned by Encrypt() and EncryptWithOpts can be ASN1 encoded,
  or of the form:
     nonce + ciphertext + tag (can be nil)
*/
func (aesKey *AESKey) Encrypt(plain []byte) ([]byte, error) {
	return aesKey.EncryptWithOpts(plain, defaultAESOpts)
}

func (aesKey *AESKey) EncryptWithOpts(plain []byte, opts *crypto.EncOpts) ([]byte, error) {
	block, err := aes.NewCipher(aesKey.Key)
	if err != nil {
		return nil, fmt.Errorf("AES encryption fails: %v", err)
	}
	var ciphertext aesCiphertext

	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:

		var msg []byte
		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			msg = util.PKCS5Padding(plain, block.BlockSize())
		default:
			return nil, fmt.Errorf("AES CBC encryption fails: invalid padding scheme [%s]", opts.EncodingType)
		}

		ciphertext.IV = make([]byte, block.BlockSize())
		if _, err := rand.Read(ciphertext.IV); err != nil {
			return nil, fmt.Errorf("AES CBC encryption fails: %v", err)
		}

		blockMode := cipher.NewCBCEncrypter(block, ciphertext.IV)
		ciphertext.Ciphertext = make([]byte, len(msg))
		blockMode.CryptBlocks(ciphertext.Ciphertext[0:], msg)
		ciphertext.Tag = nil

	case modes.BLOCK_MODE_GCM:
		gcmMode, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("AES GCM encryption fails: %v", err)
		}

		ivLength := gcmMode.NonceSize()
		ciphertext.IV = make([]byte, ivLength)
		if _, err := rand.Read(ciphertext.IV); err != nil {
			return nil, fmt.Errorf("AES GCM encryption fails: %v", err)
		}

		cipherWithTag := gcmMode.Seal(nil, ciphertext.IV, plain, nil)
		tagLength := gcmMode.Overhead()
		ciphertext.Ciphertext = cipherWithTag[0 : len(cipherWithTag)-tagLength]
		ciphertext.Tag = cipherWithTag[len(cipherWithTag)-tagLength:]

	default:
		return nil, fmt.Errorf("AES encryption fails: unknown cipher block mode [%s]", opts.BlockMode)
	}

	if opts.EnableASN1 {
		return asn1.Marshal(ciphertext)
	}
	ret := append(ciphertext.IV, ciphertext.Ciphertext...)
	ret = append(ret, ciphertext.Tag...)
	return ret, nil
}

/*
  The input ciphertext can be ASN1 encoded,
  or of the form:
     nonce + ciphertext + tag (can be nil)
*/
func (aesKey *AESKey) Decrypt(crypted []byte) ([]byte, error) {
	return aesKey.DecryptWithOpts(crypted, defaultAESOpts)
}

func (aesKey *AESKey) DecryptWithOpts(crypted []byte, opts *crypto.EncOpts) ([]byte, error) {
	var ciphertext aesCiphertext
	if opts.EnableASN1 {
		_, err := asn1.Unmarshal(crypted, &ciphertext)
		if err != nil {
			return nil, fmt.Errorf("AES decryption fails: %v", err)
		}
	}

	block, err := aes.NewCipher(aesKey.Key)
	if err != nil {
		return nil, fmt.Errorf("AES decryption fails: %v", err)
	}

	switch opts.BlockMode {
	case modes.BLOCK_MODE_CBC:
		if !opts.EnableASN1 {
			ciphertext.IV = crypted[0:block.BlockSize()]
			ciphertext.Ciphertext = crypted[block.BlockSize():]
			ciphertext.Tag = nil
		}
		blockMode := cipher.NewCBCDecrypter(block, ciphertext.IV)
		orig := make([]byte, len(ciphertext.Ciphertext))
		blockMode.CryptBlocks(orig, ciphertext.Ciphertext)

		switch opts.EncodingType {
		case modes.PADDING_PKCS5:
			orig, err = util.PKCS5UnPadding(orig)
			if err != nil {
				return nil, fmt.Errorf("AES CBC decryption fails: %v", err)
			}
			return orig, nil
		default:
			return nil, fmt.Errorf("AES CBC decryption fails: invalid padding scheme [%s]", opts.EncodingType)
		}

	case modes.BLOCK_MODE_GCM:
		gcmMode, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("AES GCM decryption fails: %v", err)
		}
		if !opts.EnableASN1 {
			ciphertext.IV = crypted[0:gcmMode.NonceSize()]
			ciphertext.Ciphertext = crypted[gcmMode.NonceSize() : len(crypted)-gcmMode.Overhead()]
			ciphertext.Tag = crypted[len(crypted)-gcmMode.Overhead():]
		}

		cipherWithTag := append(ciphertext.Ciphertext, ciphertext.Tag...)

		return gcmMode.Open(nil, ciphertext.IV, cipherWithTag, nil)

	default:
		return nil, fmt.Errorf("AES decryption fails: unknown cipher block mode [%s]", opts.BlockMode)
	}
}

func (aesKey *AESKey) Type() crypto.KeyType {
	return crypto.AES
}
