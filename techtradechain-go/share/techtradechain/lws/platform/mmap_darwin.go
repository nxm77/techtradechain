/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package platform

import (
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	MAP_PROT_READ  = syscall.PROT_READ
	MAP_PROT_WRITE = syscall.PROT_WRITE
	MAP_SHARED     = syscall.MAP_SHARED
)

func mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	return unix.Mmap(fd, offset, length, prot, flags)
}

func munmap(b []byte) error {
	return unix.Munmap(b)
}

// This is required because the unix package does not support the madvise system call on OS X.
// func madvise(b []byte, readahead bool) error {
// 	advice := unix.MADV_NORMAL
// 	if !readahead {
// 		advice = unix.MADV_RANDOM
// 	}

// 	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])),
// 		uintptr(len(b)), uintptr(advice))
// 	if e1 != 0 {
// 		return e1
// 	}
// 	return nil
// }

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
