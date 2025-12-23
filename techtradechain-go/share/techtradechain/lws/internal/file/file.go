/*
/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package file

import (
	"errors"
	"os"
	"syscall"
)

var errNoFallocate = errors.New("operation not supported")

type File struct {
	*os.File
}

func NewFile(path string) (*File, error) {
	return OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
}

func OpenFile(path string, flag int, perm os.FileMode) (*File, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	return &File{
		File: f,
	}, nil
}

// Truncate 截断文件 同普通文件Truncate
func (f *File) Truncate(size int64) error {
	var err error
	for {
		if err = f.fallocate(uint64(size)); err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		if err != syscall.ENOTSUP && err != syscall.EPERM && err != errNoFallocate {
			return err
		}
	}
	return f.File.Truncate(size)
}

func (f *File) Size() int64 {
	info, err := f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}
