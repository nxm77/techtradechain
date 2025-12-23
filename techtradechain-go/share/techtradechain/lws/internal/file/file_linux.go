/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package file

import "syscall"

func (f *File) fallocate(size uint64) error {
	return syscall.Fallocate(int(f.File.Fd()), 0, 0, int64(size))
}
