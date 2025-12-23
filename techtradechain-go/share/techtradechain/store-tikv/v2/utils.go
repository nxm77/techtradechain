/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package tikvdbprovider

// makePrefixedKey concatenate dbprefix and key
// @param []byte key
// @param []byte
// @return []byte
func makePrefixedKey(key []byte, dbPrefix []byte) []byte {
	return append(append([]byte{}, dbPrefix...), key...)
}

//func removePrefixedKey(key []byte, dbPrefix []byte) []byte {
//	if len(key) < len(dbPrefix) {
//		return key
//	}
//	return key[len(dbPrefix):]
//}

// BytesPrefix returns key range that satisfy the given prefix.
// This only applicable for the standard 'bytes comparer'.
// @param []byte prefix
// @return []byte
// @return []byte
func bytesPrefix(prefix []byte) (start []byte, end []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}
