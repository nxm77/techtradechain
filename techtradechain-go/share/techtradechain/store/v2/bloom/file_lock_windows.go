/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bloom

import (
	"fmt"
	"path/filepath"
	"syscall"
	"time"
)

type windowsFileLock struct {
	fd syscall.Handle
}

func (fl *windowsFileLock) release() error {
	return syscall.Close(fl.fd)
}

func newFileLock(b_file *bloomFile, exclusive bool, timeout time.Duration) (fl fileLock, err error) {
	file_p := filepath.Join(filepath.Dir(b_file.path), "LOCK")
	t := time.Now()
	for {
		fd, err := flock(file_p, exclusive)
		if err == nil {
			return &windowsFileLock{fd: fd}, nil
		}
		if err != syscall.EAGAIN {
			return nil, fmt.Errorf("file lock error: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
		if timeout > 0 && time.Since(t) >= timeout {
			return nil, fmt.Errorf("file lock error: %v", err)
		}
	}
}

func flock(file_p string, exclusive bool) (syscall.Handle, error) {
	pathp, err := syscall.UTF16PtrFromString(file_p)
	if err != nil {
		return 0, err
	}
	var access, shareMode uint32
	if !exclusive {
		access = syscall.GENERIC_READ
		shareMode = syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE
	} else {
		access = syscall.GENERIC_READ | syscall.GENERIC_WRITE
	}
	fd, err := syscall.CreateFile(pathp, access, shareMode, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err == syscall.ERROR_FILE_NOT_FOUND {
		fd, err = syscall.CreateFile(pathp, access, shareMode, nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	}
	return fd, err
}
