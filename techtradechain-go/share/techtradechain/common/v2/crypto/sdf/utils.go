/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sdf

import "C"
import (
	"strings"

	bccrypto "techtradechain.com/techtradechain/common/v2/crypto"
)

/* util funcs */
func CCharArrToGoSlice(buf []C.uchar) []byte {
	var ret []byte
	for i := 0; i < len(buf); i++ {
		ret = append(ret, byte(buf[i]))
	}
	return ret
}

func convertToSDFKeyType(keyType bccrypto.KeyType) SDFKeyType {
	keyTypeStr := bccrypto.KeyType2NameMap[keyType]
	if strings.Contains(keyTypeStr, "RSA") {
		return RSA
	} else if strings.Contains(keyTypeStr, "SM2") {
		return SM2
	} else if strings.Contains(keyTypeStr, "ECC") || strings.Contains(keyTypeStr, "ECDSA") {
		return ECDSA
	} else if strings.Contains(keyTypeStr, "AES") {
		return AES
	} else if strings.Contains(keyTypeStr, "SM4") {
		return SM4
	}

	return UNKNOWN
}
