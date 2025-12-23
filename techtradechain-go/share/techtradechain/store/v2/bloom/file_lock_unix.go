//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd
// +build darwin dragonfly freebsd linux netbsd openbsd

/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bloom

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

type unixFileLock struct {
	f *os.File
}

func (fl *unixFileLock) release() error {
	if fl.f != nil {
		return funlock(fl.f)
	}
	return nil
}

func newFileLock(b_file *bloomFile, exclusive bool, timeout time.Duration) (*unixFileLock, error) {
	t := time.Now()
	for {
		err := flock(b_file.file, exclusive)
		if err == nil {
			return &unixFileLock{f: b_file.file}, nil
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

func flock(f *os.File, exclusive bool) error {
	flag := syscall.LOCK_SH
	if exclusive {
		flag = syscall.LOCK_EX
	}
	return syscall.Flock(int(f.Fd()), flag|syscall.LOCK_NB)
}

func funlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
