/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest test util
package birdsnest

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"testing"
	"time"

	"techtradechain.com/techtradechain/common/v2/random/uuid"
	guuid "github.com/google/uuid"
)

// TestDir test path
const TestDir = "./data/timestamp_birds_nest"

// GetTimestampKey get timestamp key
func GetTimestampKey() Key {
	key, _ := ToTimestampKey(GenTimestampKey())
	return key
}

// GetTimestampKeyByNano get timestamp key by nanosecond
func GetTimestampKeyByNano(i int64) Key {
	key, _ := ToTimestampKey(GenTimestampKeyByNano(i))
	return key
}

// GenTimestampKey generate timestamp key
func GenTimestampKey() string {
	return GenTimestampKeyByNano(time.Now().UnixNano())
}

// GenTimestampKeyByNano generate timestamp key by nanosecond
func GenTimestampKeyByNano(nano int64) string {
	b := make([]byte, 16, 32)
	binary.BigEndian.PutUint64(b, uint64(nano))
	/*
		Read generates len(p) random bytes from the default Source and
		writes them into p. It always returns len(p) and a nil error.
		Read, unlike the Rand.Read method, is safe for concurrent use.
	*/
	b[8] = Separator
	// nolint: gosec
	_, _ = rand.Read(b[9:16])
	u := guuid.New()
	b = append(b, u[:]...)
	return hex.EncodeToString(b)
}

// GenTxId generate tx id
func GenTxId() string {
	return uuid.GetUUID() + uuid.GetUUID()
}

// GetTimestampKeys get timestamp key collections
func GetTimestampKeys(n int) []Key {
	var keys []Key
	for i := 0; i < n; i++ {
		keys = append(keys, GetTimestampKey())
	}
	return keys
}

// TestLogger test logger
type TestLogger struct {
	T *testing.T
}

// Debugf debug format
func (t TestLogger) Debugf(format string, args ...interface{}) {
	//t.T.Logf(format, args...)
}

// Errorf error format
func (t TestLogger) Errorf(format string, args ...interface{}) {
	//t.T.Errorf(format, args...)
}

// Infof info format
func (t TestLogger) Infof(format string, args ...interface{}) {
	//t.T.Logf(format, args...)
}

// Warnf warn format
func (t TestLogger) Warnf(format string, args ...interface{}) {
	//t.T.Logf(format, args...)
}
