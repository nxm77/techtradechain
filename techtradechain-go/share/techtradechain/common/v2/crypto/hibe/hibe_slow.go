//go:build !linux || !amd64
// +build !linux !amd64

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// nolint
package hibe

import (
	"io"
	"math/big"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_noamd64"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_noamd64/hibe"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_noamd64/hibe/bn256"
)

type Params = hibe.Params
type MasterKey = hibe.MasterKey
type Ciphertext = hibe.Ciphertext
type PrivateKey = hibe.PrivateKey
type G1 = bn256.G1

// EncryptHibeMsg is used to encrypt plainText by receiverIds and their paramsList
// plaintext: plain text bytes
// receiverIds: message receivers' id list, using "/" to separate hierarchy identity in each id string
// paramsList: HIBE parameters list of the message receiver, len(paramsList) should be equal to len(receiverIds),
//   paramsList[i] are the HIBE parameters of receiverIds[i]
// symKeyType: symmetric key type (aes or sm4), used to symmetric encrypt the plain text first
func EncryptHibeMsg(plaintext []byte, receiverIds []string, paramsList []*Params,
	symKeyType crypto.KeyType) (map[string]string, error) {
	return hibe_noamd64.EncryptHibeMsg(plaintext, receiverIds, paramsList, symKeyType)
}

// DecryptHibeMsg is used to decrypt the HIBE message constructed by EncryptHibeMsg
// localId: hibe Id
// hibeParams: HIBE parameters of the HIBE system to which ID belongs
// prvKey: the localId's hibe private Key
// hibeMsgMap: HIBE message encrypt by EncryptHibeMsg
// symKeyType: symmetric key type (aes or sm4), used to symmetric encrypt the plain text first
func DecryptHibeMsg(localId string, hibeParams *Params, prvKey *PrivateKey,
	hibeMsgMap map[string]string, symKeyType crypto.KeyType) ([]byte, error) {
	return hibe_noamd64.DecryptHibeMsg(localId, hibeParams, prvKey, hibeMsgMap, symKeyType)
}

// ValidateId is used to validate id format
func ValidateId(id string) error {
	return hibe_noamd64.ValidateId(id)
}

func Setup(random io.Reader, l int) (*Params, MasterKey, error) {
	return hibe.Setup(random, l)
}

func KeyGenFromMaster(random io.Reader, params *Params, master MasterKey, id []*big.Int) (*PrivateKey, error) {
	return hibe.KeyGenFromMaster(random, params, master, id)
}

func KeyGenFromParent(random io.Reader, params *Params, parent *PrivateKey, id []*big.Int) (*PrivateKey, error) {
	return hibe.KeyGenFromParent(random, params, parent, id)
}

func Encrypt(random io.Reader, params *Params, id []*big.Int, message *bn256.GT) (*Ciphertext, error) {
	return hibe.Encrypt(random, params, id, message)
}

func Decrypt(key *PrivateKey, ciphertext *Ciphertext) *bn256.GT {
	return hibe.Decrypt(key, ciphertext)
}

// IdStr2HibeId construct HibeId according to id
func IdStr2HibeId(id string) ([]string, []*big.Int) {
	return hibe_noamd64.IdStr2HibeId(id)
}
