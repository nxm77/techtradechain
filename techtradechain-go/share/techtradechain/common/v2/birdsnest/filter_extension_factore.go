/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest filter extension factory
package birdsnest

import (
	"sync"
)

var (
	// ErrFilterExtensionNotSupportMessage error filter extension not support message
	ErrFilterExtensionNotSupportMessage = "filter extension not support type: %v"
)

type factory struct {
}

var once sync.Once
var _instance *factory

// Factory return the global tx filter factory.
// nolint: revive
func Factory() *factory {
	once.Do(func() { _instance = new(factory) })
	return _instance
}

func (cf *factory) New(fet FilterExtensionType) (FilterExtension, error) {
	switch fet {
	case FilterExtensionType_FETDefault:
		return NewDefaultFilterExtension(), nil
	case FilterExtensionType_FETTimestamp:
		return NewTimestampFilterExtension(), nil
	default:
		return nil, NewError(ErrFilterExtensionNotSupportMessage, fet)
	}
}
