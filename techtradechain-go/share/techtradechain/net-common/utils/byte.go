/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"bytes"
	"encoding/binary"
)

// Uint64ToBytes parse uint64 to 8 bytes.
func Uint64ToBytes(n uint64) []byte {
	x := n
	bytesBuffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

// BytesToUint64 parse 8 bytes to uint64.
func BytesToUint64(b []byte) uint64 {
	bytesBuffer := bytes.NewBuffer(b)
	var x uint64
	_ = binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

// Uint32ToBytes parse uint32 to 4 bytes.
func Uint32ToBytes(n uint32) []byte {
	x := n
	bytesBuffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

// BytesToUint32 parse 4 bytes to uint32.
func BytesToUint32(b []byte) uint32 {
	bytesBuffer := bytes.NewBuffer(b)
	var x uint32
	_ = binary.Read(bytesBuffer, binary.BigEndian, &x)
	return x
}

// IntToBytes parse an int value to 8 bytes.
func IntToBytes(n int) []byte {
	x := int64(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

// BytesToInt parse 8 bytes to an int value.
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int64
	_ = binary.Read(bytesBuffer, binary.BigEndian, &x)

	return int(x)
}
