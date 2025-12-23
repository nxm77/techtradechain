/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package platform

func Mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	return mmap(fd, offset, length, prot, flags)
}

// Munmap unmaps a previously mapped slice.
func Munmap(b []byte) error {
	return munmap(b)
}

// Msync would call sync on the mmapped data.
func Msync(b []byte) error {
	return msync(b)
}
