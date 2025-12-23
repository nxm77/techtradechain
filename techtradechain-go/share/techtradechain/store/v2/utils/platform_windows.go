//go:build windows
// +build windows

/*
* Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
*
* SPDX-License-Identifier: Apache-2.0
 */

package utils

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
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

	const LOCKFILE_EXCLUSIVE_LOCK = 2
	fileHandle := windows.Handle(f.Fd())
	mode := uint32(LOCKFILE_EXCLUSIVE_LOCK)
	if err := windows.LockFileEx(fileHandle, mode, 0, 1, 0, &windows.Overlapped{}); err != nil {
		return fmt.Errorf("failed to lock file: %s", f.Name())
	}

	return nil
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

	fileHandle := windows.Handle(f.Fd())
	if err := windows.UnlockFileEx(fileHandle, 0, 0, 1, &windows.Overlapped{}); err != nil {
		return fmt.Errorf("failed to unlock file: %s", f.Name())
	}

	return nil
}
