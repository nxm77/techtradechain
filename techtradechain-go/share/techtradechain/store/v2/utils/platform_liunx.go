//go:build !windows
// +build !windows

// Package utils package
package utils

/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import (
	"os"
	"syscall"
)

// FileLocker add next time
//
//	@Description:
//	@param f
//	@return error
func FileLocker(f *os.File) error {
	if f == nil {
		return nil
	}
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

// FileUnLocker add next time
//
//	@Description:
//	@param f
//	@return error
func FileUnLocker(f *os.File) error {
	if f == nil {
		return nil
	}
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
