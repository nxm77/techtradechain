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
	return syscall.Mmap(fd, offset, length, prot, flags)
}

func munmap(data []byte) error {
	return syscall.Munmap(data)
}

func madvise(b []byte, readahead bool) error {
	flags := unix.MADV_NORMAL
	if !readahead {
		flags = unix.MADV_RANDOM
	}
	return unix.Madvise(b, flags)
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
