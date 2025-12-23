/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bytehelper

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

// BytesToInt le bytes to int32, little endian
func BytesToInt(b []byte) (int32, error) {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	err := binary.Read(bytesBuffer, binary.LittleEndian, &x)
	if err != nil {
		return -1, err
	}
	return x, nil
}

// BytesToInt64 le bytes to int64, little endian
func BytesToInt64(b []byte) (int64, error) {
	bytesBuffer := bytes.NewBuffer(b)
	var x int64
	err := binary.Read(bytesBuffer, binary.LittleEndian, &x)
	if err != nil {
		return -1, err
	}
	return x, nil
}

// IntToBytes int32 to le bytes, little endian
func IntToBytes(x int32) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.LittleEndian, x)
	if err != nil {
		return nil
	}
	return bytesBuffer.Bytes()
}

// Int64ToBytes int64 to le bytes, little endian
func Int64ToBytes(x int64) ([]byte, error) {
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.LittleEndian, x)
	if err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

// BytesToUint64 le bytes to uint64, little endian
func BytesToUint64(b []byte) (uint64, error) {
	bytesBuffer := bytes.NewBuffer(b)
	var x uint64
	err := binary.Read(bytesBuffer, binary.LittleEndian, &x)
	if err != nil {
		return 0, err
	}
	return x, nil
}

// Uint64ToBytes uint64 to le bytes, little endian
func Uint64ToBytes(x uint64) ([]byte, error) {
	bytesBuffer := bytes.NewBuffer([]byte{})
	err := binary.Write(bytesBuffer, binary.LittleEndian, x)
	if err != nil {
		return nil, err
	}
	return bytesBuffer.Bytes(), nil
}

// BytesPrefix returns key range that satisfy the given prefix.
// This only applicable for the standard 'bytes comparer'.
func BytesPrefix(prefix []byte) ([]byte, []byte) {
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

// StringToBytes converts string to byte slice without a memory allocation.
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

// BytesToString converts byte slice to string without a memory allocation.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
