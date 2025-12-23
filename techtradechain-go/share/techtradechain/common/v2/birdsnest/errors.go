/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest error
package birdsnest

import (
	"fmt"
)

// SprintfError error
type SprintfError string

// Error error information
func (s SprintfError) Error() string {
	return string(s)
}

// NewError new error
func NewError(s string, args ...interface{}) SprintfError {
	return SprintfError(fmt.Sprintf(s, args...))
}
