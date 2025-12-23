//go:build linux && amd64
// +build linux,amd64

/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hibe

import (
	"io"
	"math/big"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_amd64"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_amd64/hibe"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_amd64/hibe/bn256"
)

// Params represents the system parameters for a hierarchy.
type Params = hibe.Params

// MasterKey represents the key for a hierarchy that can create a key for any
// element.
type MasterKey = hibe.MasterKey

// Ciphertext represents an encrypted message.
type Ciphertext = hibe.Ciphertext

// PrivateKey represents a key for an ID in a hierarchy that can decrypt
// messages encrypted with that ID and issue keys for children of that ID in
// the hierarchy.
type PrivateKey = hibe.PrivateKey

// G1 is an abstract cyclic group. The zero value is suitable for use as the
// output of an operation, but cannot be used as an input.
type G1 = bn256.G1

// EncryptHibeMsg is used to encrypt plainText by receiverIds and their paramsList
// plaintext: plain text bytes
// receiverIds: message receivers' id list, using "/" to separate hierarchy identity in each id string
// paramsList: HIBE parameters list of the message receiver, len(paramsList) should be equal to len(receiverIds),
//   paramsList[i] are the HIBE parameters of receiverIds[i]
// symKeyType: symmetric key type (aes or sm4), used to symmetric encrypt the plain text first
func EncryptHibeMsg(plaintext []byte, receiverIds []string, paramsList []*Params,
	symKeyType crypto.KeyType) (map[string]string, error) {
	return hibe_amd64.EncryptHibeMsg(plaintext, receiverIds, paramsList, symKeyType)

}

// DecryptHibeMsg is used to decrypt the HIBE message constructed by EncryptHibeMsg
// localId: hibe Id
// hibeParams: HIBE parameters of the HIBE system to which ID belongs
// prvKey: the localId's hibe private Key
// hibeMsgMap: HIBE message encrypt by EncryptHibeMsg
// symKeyType: symmetric key type (aes or sm4), used to symmetric encrypt the plain text first
func DecryptHibeMsg(localId string, hibeParams *Params, prvKey *PrivateKey,
	hibeMsgMap map[string]string, symKeyType crypto.KeyType) ([]byte, error) {
	return hibe_amd64.DecryptHibeMsg(localId, hibeParams, prvKey, hibeMsgMap, symKeyType)
}

// Setup generates the system parameters, (hich may be made visible to an
// adversary. The parameter "l" is the maximum depth that the hierarchy will
// support.
func Setup(random io.Reader, l int) (*Params, MasterKey, error) {
	return hibe.Setup(random, l)
}

// KeyGenFromMaster generates a key for an ID using the master key.
func KeyGenFromMaster(random io.Reader, params *Params, master MasterKey, id []*big.Int) (*PrivateKey, error) {
	return hibe.KeyGenFromMaster(random, params, master, id)
}

// KeyGenFromParent generates a key for an ID using the private key of the
// parent of ID in the hierarchy. Using a different parent will result in
// undefined behavior.
func KeyGenFromParent(random io.Reader, params *Params, parent *PrivateKey, id []*big.Int) (*PrivateKey, error) {
	return hibe.KeyGenFromParent(random, params, parent, id)
}

// Encrypt converts the provided message to ciphertext, using the provided ID
// as the public key.
func Encrypt(random io.Reader, params *Params, id []*big.Int, message *bn256.GT) (*Ciphertext, error) {
	return hibe.Encrypt(random, params, id, message)
}

// Decrypt recovers the original message from the provided ciphertext, using
// the provided private key.
func Decrypt(key *PrivateKey, ciphertext *Ciphertext) *bn256.GT {
	return hibe.Decrypt(key, ciphertext)
}

// ValidateId is used to validate id format
func ValidateId(id string) error {
	return hibe_amd64.ValidateId(id)
}

// IdStr2HibeId construct HibeId according to id
func IdStr2HibeId(id string) ([]string, []*big.Int) {
	return hibe_amd64.IdStr2HibeId(id)
}
