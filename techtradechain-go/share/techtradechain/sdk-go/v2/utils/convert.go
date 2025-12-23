/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"encoding/binary"
	"errors"
)

// U64ToBytes uint64 to bytes in little endian
func U64ToBytes(i uint64) []byte {
	var b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

// BytesToU64 bytes to uint64 in little endian
func BytesToU64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, errors.New("invalid uint64 bytes")
	}

	return binary.LittleEndian.Uint64(b), nil
}

// I64ToBytes int64 to bytes in little endian
func I64ToBytes(i int64) []byte {
	var b = make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

// BytesToI64 bytes to int64 in little endian
func BytesToI64(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, errors.New("invalid int64 bytes")
	}

	return int64(binary.LittleEndian.Uint64(b)), nil
}
