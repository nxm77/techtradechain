/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest filter extension
package birdsnest

import (
	"encoding/binary"
	"errors"

	"go.uber.org/atomic"
)

var (
	// ErrKeyTimeIsNotInTheFilterRange Not error; Key time is not in the filter range
	ErrKeyTimeIsNotInTheFilterRange = errors.New("key time is not in the filter range")
)

// ExtensionDeserialize Extension deserialize
func ExtensionDeserialize(bytes []byte) (FilterExtension, error) {
	extensionType := FilterExtensionType(binary.BigEndian.Uint64(bytes[:8]))
	switch extensionType {
	case FilterExtensionType_FETDefault:
		return DeserializeDefault(), nil
	case FilterExtensionType_FETTimestamp:
		return DeserializeTimestamp(bytes)
	default:
		return nil, NewError(ErrFilterExtensionNotSupportMessage, extensionType)
	}
}

// DefaultFilterExtension default filter extension
type DefaultFilterExtension struct {
}

// NewDefaultFilterExtension new default filter extension
func NewDefaultFilterExtension() *DefaultFilterExtension {
	return &DefaultFilterExtension{}
}

// Validate validate
func (d DefaultFilterExtension) Validate(Key, bool) error {
	return nil
}

// Store store
func (d DefaultFilterExtension) Store(Key) error {
	return nil
}

// Serialize serialize
func (d DefaultFilterExtension) Serialize() []byte {
	var type0 = make([]byte, 8)
	binary.BigEndian.PutUint64(type0, uint64(FilterExtensionType_FETDefault))

	return type0
}

// DeserializeDefault new default deserialize extension
func DeserializeDefault() FilterExtension {
	return &DefaultFilterExtension{}
}

// TimestampFilterExtension timestamp filter extension
type TimestampFilterExtension struct {
	// firstTimestamp start time
	firstTimestamp *atomic.Int64
	// lastTimestamp end time
	lastTimestamp *atomic.Int64
}

// NewTimestampFilterExtension new timestamp filter extension
func NewTimestampFilterExtension() FilterExtension {
	return &TimestampFilterExtension{
		firstTimestamp: atomic.NewInt64(0),
		lastTimestamp:  atomic.NewInt64(0),
	}
}

// Serialize serialize
func (t *TimestampFilterExtension) Serialize() []byte {
	var type0 = make([]byte, 8)
	binary.BigEndian.PutUint64(type0, uint64(FilterExtensionType_FETTimestamp))

	var first = make([]byte, 8)
	binary.BigEndian.PutUint64(first, uint64(t.firstTimestamp.Load()))

	var last = make([]byte, 8)
	binary.BigEndian.PutUint64(last, uint64(t.lastTimestamp.Load()))

	var result []byte
	result = append(result, type0...)
	result = append(result, first...)
	result = append(result, last...)
	return result
}

// Validate validate
func (t *TimestampFilterExtension) Validate(key Key, full bool) error {
	nano := key.GetNano()
	if full {
		first := t.firstTimestamp.Load()
		if first != 0 {
			if nano < first {
				return ErrKeyTimeIsNotInTheFilterRange
			}
			if nano > t.lastTimestamp.Load() {
				return ErrKeyTimeIsNotInTheFilterRange
			}
		}
	}
	return nil
}

// Store store start time and end time
func (t *TimestampFilterExtension) Store(key Key) error {
	split, err := key.Parse()
	if err != nil {
		return err
	}
	nano := int64(binary.LittleEndian.Uint64(split[0]))
	//timestamp := nano / time.Millisecond.Nanoseconds()
	if t.firstTimestamp.Load() == 0 {
		t.firstTimestamp.Store(nano)
	}
	if nano < t.firstTimestamp.Load() {
		t.firstTimestamp.Store(nano)
	}
	if nano > t.lastTimestamp.Load() {
		t.lastTimestamp.Store(nano)
	}
	return nil
}

// DeserializeTimestamp deserialize timestamp by bytes
func DeserializeTimestamp(bytes []byte) (*TimestampFilterExtension, error) {
	t := &TimestampFilterExtension{
		firstTimestamp: atomic.NewInt64(0),
		lastTimestamp:  atomic.NewInt64(0),
	}
	if len(bytes) != 24 {
		return nil, ErrKeyCannotBeEmpty
	}

	t.firstTimestamp.Store(int64(binary.BigEndian.Uint64(bytes[8:16])))
	t.lastTimestamp.Store(int64(binary.BigEndian.Uint64(bytes[16:24])))
	return t, nil
}
