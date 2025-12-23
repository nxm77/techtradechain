/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hibe_amd64

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/common/v2/crypto/hibe/hibe_amd64/hibe"
	"techtradechain.com/techtradechain/common/v2/crypto/sym"
)

// ValidateId is used to validate id format
func ValidateId(id string) error {
	if id == "" {
		return errors.New("invalid parameters, id is nil")
	}

	idStrList := strings.Split(id, "/")

	for _, s := range idStrList {
		if s == "" {
			return fmt.Errorf("invalid parameters, id: %s, format error, only like : \"A/B/C\" can be used", id)
		}

		if strings.Contains(s, " ") {
			return fmt.Errorf("invalid parameters, id: %s, format error, only like : \"A/B/C\" can be used", id)
		}
	}
	return nil
}

// refineIdsAndParams Remove redundant data (Id)
// Only the lowest level ID is reserved for the ID with superior subordinate relationship
func refineIdsAndParams(ids []string, paramsList []*hibe.Params) ([]string, []*hibe.Params, error) {
	refinedReceiverIds := make([]string, 1)
	refinedReceiverIds[0] = ids[0]
	refinedParamsList := make([]*hibe.Params, 1)
	refinedParamsList[0] = paramsList[0]

	for i := 1; i < len(ids); i++ {
		matched := false
		for j, rId := range refinedReceiverIds {

			iIdSlice := strings.Split(ids[i], "/")
			iLen := len(iIdSlice)
			rIdSlice := strings.Split(rId, "/")
			rLen := len(rIdSlice)

			// 1.They do not have a subordinate relationship
			if iLen == rLen {
				continue
			}

			if rLen > iLen { // 2. refinedReceiverIds[j] has prefix ids[i]
				matched = true
				for k := 0; k < iLen; k++ {
					if iIdSlice[k] != rIdSlice[k] {
						matched = false
						break
					}
				}

				// they have different parameters, return error
				if matched && !reflect.DeepEqual(paramsList[i].Marshal(), refinedParamsList[j].Marshal()) {
					return nil, nil, fmt.Errorf("ID [%s] is matched, but Params are different, please check it",
						ids[i])
				}
			} else { // 3. ids[i] has prefix refinedReceiverIds[j]
				matched = true
				for k := 0; k < rLen; k++ {
					if iIdSlice[k] != rIdSlice[k] {
						matched = false
						break
					}
				}

				// they have different parameters, return error
				if matched && !reflect.DeepEqual(paramsList[i].Marshal(), refinedParamsList[j].Marshal()) {
					return nil, nil, fmt.Errorf("ID [%s] is matched, but Params are different, please check it",
						ids[i])
				}

				if matched {
					refinedReceiverIds[j] = ids[i]
					refinedParamsList[j] = paramsList[i]
					break
				}

			}
		}

		if !matched {
			refinedReceiverIds = append(refinedReceiverIds, ids[i])
			refinedParamsList = append(refinedParamsList, paramsList[i])
		}
	}

	return refinedReceiverIds, refinedParamsList, nil
}

// idStrList2HibeIds construct HibeId list according to id list
func idStrList2HibeIds(idList []string) [][]*big.Int {
	// construct ids []*big.Int
	hibeIds := make([][]*big.Int, len(idList))

	for i, idStr := range idList {
		_, hibeIds[i] = IdStr2HibeId(idStr)
	}

	return hibeIds
}

// IdStr2HibeId construct HibeId according to id
func IdStr2HibeId(id string) ([]string, []*big.Int) {
	// idStr eg: "org1/ou1" -> ["org1", "ou1"]
	strId := strings.Split(id, "/")

	// idsStr -> hibeId
	hibeIds := make([]*big.Int, len(strId))
	for i, value := range strId {
		hashedStrId := sha256.Sum256([]byte(value))
		bigIdBytes := hashedStrId[:]
		bigId := new(big.Int)
		bigId.SetBytes(bigIdBytes)
		hibeIds[i] = bigId
	}

	return strId, hibeIds
}

// generateSymKeyFromGtBytes is used to generate symmetric key according to gtBytes and symKeyType
func generateSymKeyFromGtBytes(gtBytes []byte, symKeyType crypto.KeyType) (crypto.SymmetricKey, error) {
	var symKey crypto.SymmetricKey

	if symKeyType == crypto.AES {
		// not gm
		gtBytesHash, err := hash.Get(crypto.HASH_TYPE_SHA256, gtBytes)
		if err != nil {
			return nil, err
		}
		symKey, err = sym.GenerateSymKey(crypto.AES, gtBytesHash)
		if err != nil {
			return nil, err
		}

	} else if symKeyType == crypto.SM4 {
		// gm
		gtBytesHash, err := hash.Get(crypto.HASH_TYPE_SM3, gtBytes)
		if err != nil {
			return nil, err
		}
		symKey, err = sym.GenerateSymKey(crypto.SM4, gtBytesHash[:16])
		if err != nil {
			return nil, err
		}

	} else {
		return nil, fmt.Errorf("invalid parameters, unsupported symmetric encryption algorithm type : %d", symKeyType)
	}

	return symKey, nil
}
