//go:build windows
// +build windows

/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package platform

import (
	"errors"
)

var (
	MAP_PROT_READ  = 0
	MAP_PROT_WRITE = 0
	MAP_SHARED     = 0
)

var (
	errNotImp = errors.New("not implement on windows platform")
)

func mmap(fd int, offset int64, length int, prot int, flags int) (data []byte, err error) {
	return nil, errNotImp
}

func munmap(b []byte) (err error) {
	return errNotImp
}

func msync(b []byte) error {
	return errNotImp
}
