//go:build !linux
// +build !linux

/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package file

func (f *File) fallocate(size uint64) error {
	return errNoFallocate
}
