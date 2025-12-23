/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest key
package birdsnest

import (
	"encoding/hex"
	"errors"
)

var (
	// SeparatorString -
	SeparatorString = "-"
	// Separator techtradechain ca
	Separator = byte(202)

	// ErrKeyLengthCannotBeZero key length cannot be zero error
	ErrKeyLengthCannotBeZero = errors.New("the key length cannot be 0")
	// ErrTimestampKeyLengthCannotLessNine timestamp key length cannot less 9 error
	ErrTimestampKeyLengthCannotLessNine = errors.New("the timestamp key length cannot less 9")
	// ErrNotTimestampKey not timestamp key error
	ErrNotTimestampKey = errors.New("not timestamp txid")
	// ErrTimestampKeyIsInvalid timestamp key is invalid
	ErrTimestampKeyIsInvalid = errors.New("TxId nanosecond is invalid")
)

// Key filter key
type Key interface {
	// Parse the key
	Parse() ([][]byte, error)
	// Key bytes
	Key() []byte
	// Len The length of the key
	Len() int
	// String key to string
	String() string
	// GetNano get nanosecond
	GetNano() int64
}

// TimestampKey Converting TxId directly using TimestampKey is not allowed, see ToTimestampKey
type TimestampKey []byte

// ToTimestampKey strings txid to timestamp key
// It believes that you should be a string. For the format like "0 000",
// it is not in hexadecimal format. Hexadecimal requires one character to occupy half a byte,
// and at least two characters are needed for a byte.
// For "00", it will be considered as hexadecimal, which is just "0".
// Both "00" and "000" will be considered as decimal, and it will only return an error.
// Later on, it will directly query the database, so there's no issue.
// "00" will be considered as hexadecimal, and if the length of the split is insufficient later on, it will panic.
func ToTimestampKey(txId string) (TimestampKey, error) {
	b, err := hex.DecodeString(txId)
	if err != nil {
		return nil, err
	}
	// 处理类似于00这种情况
	if len(b) < 9 {
		return nil, ErrTimestampKeyLengthCannotLessNine
	}
	if b[8] != Separator {
		return nil, ErrNotTimestampKey
	}
	if bytes2nano(b[:8]) < 0 {
		return nil, ErrTimestampKeyIsInvalid
	}
	key := TimestampKey(b)
	return key, nil
}

// ToStrings TimestampKey to string
func ToStrings(keys []Key) []string {
	result := make([]string, len(keys))
	for i := range keys {
		result[i] = keys[i].String()
	}
	return result
}

// Len length
func (k TimestampKey) Len() int {
	return len(k)
}

// Key to bytes
func (k TimestampKey) Key() []byte {
	return k
}

func (k TimestampKey) String() string {
	return hex.EncodeToString(k)
}

// GetNano get nanosecond
func (k TimestampKey) GetNano() int64 {
	return bytes2nano(k[:8])
}

// Parse parse
func (k TimestampKey) Parse() ([][]byte, error) {
	if len(k) == 0 {
		return nil, ErrKeyLengthCannotBeZero
	}
	if k[8] != Separator {
		return nil, ErrNotTimestampKey
	}
	if k.GetNano() < 0 {
		return nil, ErrTimestampKeyIsInvalid
	}
	return [][]byte{k[:8], k[8:32]}, nil
}
