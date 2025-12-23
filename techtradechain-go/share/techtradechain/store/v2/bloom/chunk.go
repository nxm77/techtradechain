/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bloom

import "unsafe"

const chunkSize = 512

type chunk struct {
	ptr uintptr
}

func (c *chunk) meta() *bloomMeta {
	return (*bloomMeta)(unsafe.Pointer(&c.ptr))
}

func (c *chunk) header() *fileHeader {
	return (*fileHeader)(unsafe.Pointer(&c.ptr))
}

func newEmptyChunk() *chunk {
	buf := make([]byte, chunkSize)
	return (*chunk)(unsafe.Pointer(&buf[0]))
}
