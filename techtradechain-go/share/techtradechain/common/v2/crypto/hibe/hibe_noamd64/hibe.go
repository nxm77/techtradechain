/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hibe_noamd64

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_noamd64/hibe"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_noamd64/hibe/bn256"
)

const (
	// hibe message's cipher text key
	hibeMsgCipherTextKey = "CT"
)

var ErrUnsupported = errors.New("hibe: unsupported")

// EncryptHibeMsg is used to encrypt plainText by receiverIds and their paramsList
// plaintext: plain text bytes
// receiverIds: message receivers' id list, using "/" to separate hierarchy identity in each id string
// paramsList: HIBE parameters list of the message receiver, len(paramsList) should be equal to len(receiverIds),
//   paramsList[i] are the HIBE parameters of receiverIds[i]
// symKeyType: symmetric key type (aes or sm4), used to symmetric encrypt the plain text first
func EncryptHibeMsg(plaintext []byte, receiverIds []string, paramsList []*hibe.Params,
	symKeyType crypto.KeyType) (map[string]string, error) {
	// input parameter validation
	if len(plaintext) == 0 {
		return nil, errors.New("invalid parameters, plaintext is nil")
	}

	if len(receiverIds) == 0 {
		return nil, errors.New("invalid parameters, receiverIds is nil")
	}
	for _, id := range receiverIds {
		if err := ValidateId(id); err != nil {
			return nil, err
		}
	}

	if len(paramsList) == 0 {
		return nil, errors.New("invalid parameters, paramsList is nil")
	}

	if len(receiverIds) != len(paramsList) {
		return nil, errors.New("invalid parameters, receiverIds and paramsList do not match, place check them")
	}

	if symKeyType != crypto.AES && symKeyType != crypto.SM4 {
		return nil, fmt.Errorf("invalid parameters, unsupported symmetric encryption algorithm type : %d", symKeyType)
	}

	// generate symmetric encryption (like AES, SM4) Key
	// generate a random point from GT
	_, randG1, err := bn256.RandomG1(rand.Reader)
	if err != nil {
		return nil, err
	}
	_, randG2, err := bn256.RandomG2(rand.Reader)
	if err != nil {
		return nil, err
	}
	gt := bn256.Pair(randG1, randG2)
	gtBytes := gt.Marshal()
	symKey, err := generateSymKeyFromGtBytes(gtBytes, symKeyType)
	if err != nil {
		return nil, err
	}

	// use sym key to encrypt plaintext
	encryptedMessage, err := symKey.Encrypt(plaintext)
	if err != nil {
		return nil, err
	}
	encryptedMessageStr := base64.StdEncoding.EncodeToString(encryptedMessage)

	// Remove redundant data (Id)
	// Only the lowest level ID is reserved
	refinedReceiverIds, refinedParamsList, err := refineIdsAndParams(receiverIds, paramsList)
	if err != nil {
		return nil, err
	}

	hibeIds := idStrList2HibeIds(refinedReceiverIds)

	// ciphertext []byte -> string
	ciphertextsStr := make([]string, len(refinedReceiverIds))
	for i, id := range hibeIds {
		ciphertext, err := hibe.Encrypt(rand.Reader, refinedParamsList[i], id[:], gt)
		if err != nil {
			return nil, err
		}
		ciphertextsStr[i] = base64.StdEncoding.EncodeToString(ciphertext.Marshal())
	}

	hibeMsgMap := make(map[string]string)
	hibeMsgMap[hibeMsgCipherTextKey] = encryptedMessageStr
	for i, ciphertextStr := range ciphertextsStr {
		hibeMsgMap[refinedReceiverIds[i]] = ciphertextStr
	}

	return hibeMsgMap, nil
}

// DecryptHibeMsg is used to decrypt the HIBE message constructed by EncryptHibeMsg
// localId: hibe Id
// hibeParams: HIBE parameters of the HIBE system to which ID belongs
// prvKey: the localId's hibe private Key
// hibeMsgMap: HIBE message encrypt by EncryptHibeMsg
// symKeyType: symmetric key type (aes or sm4), used to symmetric encrypt the plain text first
func DecryptHibeMsg(localId string, hibeParams *hibe.Params, prvKey *hibe.PrivateKey,
	hibeMsgMap map[string]string, symKeyType crypto.KeyType) ([]byte, error) {
	// input parameter validation
	if err := ValidateId(localId); err != nil {
		return nil, err
	}

	if hibeParams == nil {
		return nil, errors.New("invalid parameters, hibeParams is nil")
	}

	if prvKey == nil {
		return nil, errors.New("invalid parameters, prvKey is nil")
	}

	if hibeMsgMap == nil {
		return nil, errors.New("invalid parameters, hibeMsgMap is nil")
	}

	if symKeyType != crypto.AES && symKeyType != crypto.SM4 {
		return nil, fmt.Errorf("invalid parameters, unsupported symmetric encryption algorithm type : %d", symKeyType)
	}

	matchedId := ""
	for id := range hibeMsgMap {
		if id == hibeMsgCipherTextKey {
			continue
		}

		if strings.HasPrefix(id, localId) {
			matchedId = id
			break
		}
	}

	if matchedId == "" {
		return nil, errors.New("no permission")
	}

	matchedPrvKey := new(hibe.PrivateKey)
	if matchedId != localId {
		matchedIdStr, hibeIds := IdStr2HibeId(matchedId)

		localIdStrLen := len(strings.Split(localId, "/"))
		var err error
		for i := localIdStrLen + 1; i <= len(matchedIdStr); i++ {
			prvKey, err = hibe.KeyGenFromParent(rand.Reader, hibeParams, prvKey, hibeIds[:i])
			if err != nil {
				return nil, err
			}
		}
		matchedPrvKey = prvKey
	}

	matchedPrvKey = prvKey

	// get Gt
	encryptGt := &hibe.Ciphertext{}
	encryptGtBytes, err := base64.StdEncoding.DecodeString(hibeMsgMap[matchedId])
	if err != nil {
		return nil, err
	}
	if encryptGtBytes == nil {
		return nil, errors.New("no permission")
	}

	encryptGt, ok := encryptGt.Unmarshal(encryptGtBytes)
	if !ok {
		return nil, errors.New("encryptGt.Unmarshal failed, please check it")
	}

	// get GT
	gt := hibe.Decrypt(matchedPrvKey, encryptGt)

	// generate symmetric encryption (like AES, SM4) Key
	// generate a random point from GT
	gtBytes := gt.Marshal()
	symKey, err := generateSymKeyFromGtBytes(gtBytes, symKeyType)
	if err != nil {
		return nil, err
	}

	// decrypt messageBytes
	// encryptedMessage -> []byte
	encryptedMessageBytes, err := base64.StdEncoding.DecodeString(hibeMsgMap[hibeMsgCipherTextKey])
	if err != nil {
		return nil, err
	}

	// use sym key to encrypt plaintext
	message, err := symKey.Decrypt(encryptedMessageBytes)
	if err != nil {
		return nil, err
	}

	return message, nil
}
