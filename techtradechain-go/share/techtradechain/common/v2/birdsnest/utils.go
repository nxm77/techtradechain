/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest utils
package birdsnest

import (
	"encoding/binary"
	"time"
)

// ToTimestampKeysAndNormalKeys string to TimestampKey return timestampKeys and normalKeys
func ToTimestampKeysAndNormalKeys(key []string) (timestampKeys []Key, normalKeys []Key) {
	for i := 0; i < len(key); i++ {
		timestampKey, err := ToTimestampKey(key[i])
		if err != nil {
			normalKeys = append(normalKeys, TimestampKey(key[i]))
		} else {
			timestampKeys = append(timestampKeys, timestampKey)
		}
	}
	return
}

// CurrentTimestampNano get current timestamp nanosecond
func CurrentTimestampNano() int64 {
	return time.Now().UnixNano()
}

// bytes2nano bytes to nanosecond
func bytes2nano(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
