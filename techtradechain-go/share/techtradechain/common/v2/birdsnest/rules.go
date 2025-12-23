/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest rules
package birdsnest

import (
	"errors"
	"time"
)

// Rule rule
type Rule interface {
	Validate(Key) error
}

var (
	// ErrKeyItsSoLongAgoError key it's so long ago error
	ErrKeyItsSoLongAgoError = errors.New("key is out of the range")
)

// AbsoluteExpireTimeRule absolute expire time rule
type AbsoluteExpireTimeRule struct {
	absoluteExpireTime int64
	log                Logger
}

// Validate timestamp key
func (r AbsoluteExpireTimeRule) Validate(key Key) error {
	nano := key.GetNano()
	seconds := time.Now().UnixNano()
	start := seconds - r.absoluteExpireTime
	end := seconds + r.absoluteExpireTime
	if nano < start || nano > end {
		r.log.Warnf("key %v is out of the range %v-%v", key.String(), start, end)
		return ErrKeyItsSoLongAgoError
	}
	return nil
}

// NewAETRule new absolute expire time rule
func NewAETRule(absoluteExpireTime int64, logger Logger) AbsoluteExpireTimeRule {
	return AbsoluteExpireTimeRule{absoluteExpireTime: absoluteExpireTime * time.Second.Nanoseconds(), log: logger}
}
