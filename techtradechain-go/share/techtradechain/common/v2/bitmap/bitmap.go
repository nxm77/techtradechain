/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bitmap

import (
	"bytes"
	"fmt"
)

// Bitmap is a bitmap implementation
type Bitmap struct {
	words []uint64
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// fillLen fills the bitmap to the length
// @param length
func (bitmap *Bitmap) fillLen(length int) {
	for len(bitmap.words) < length {
		bitmap.words = append(bitmap.words, 0)
	}
}

// Has returns true if the num bit is 1
// @param num
// @return bool
func (bitmap *Bitmap) Has(num int) bool {
	word, bit := num/64, uint(num%64)
	return word < len(bitmap.words) && (bitmap.words[word]&(1<<bit)) != 0
}

// Set sets the num bit to 1
// @param num
// @return *Bitmap
func (bitmap *Bitmap) Set(num int) *Bitmap {
	word, bit := num/64, uint(num%64)
	for word >= len(bitmap.words) {
		bitmap.words = append(bitmap.words, 0)
	}
	// 判断num是否已经存在bitmap中
	if bitmap.words[word]&(1<<bit) == 0 {
		bitmap.words[word] |= 1 << bit
	}
	return bitmap
}

// InterExist returns true if the intersection of two bitmaps is not empty
// @param bitmap2
// @return bool
func (bitmap *Bitmap) InterExist(bitmap2 *Bitmap) bool {
	if bitmap2 == nil {
		return false
	}
	for i := 0; i < min(len(bitmap.words), len(bitmap2.words)); i++ {
		if bitmap.words[i]&bitmap2.words[i] > 0 {
			return true
		}
	}
	return false
}

// Or returns the union of two bitmaps
// @param bitmap2
func (bitmap *Bitmap) Or(bitmap2 *Bitmap) {
	if bitmap2 == nil {
		return
	}
	bitmap.fillLen(len(bitmap2.words))
	for i := 0; i < len(bitmap2.words); i++ {
		bitmap.words[i] = bitmap.words[i] | bitmap2.words[i]
	}
}

// Xor returns the xor of two bitmaps
// @param bitmap2
func (bitmap *Bitmap) Xor(bitmap2 *Bitmap) {
	if bitmap2 == nil {
		return
	}
	bitmap.fillLen(len(bitmap2.words))
	for i := 0; i < len(bitmap2.words); i++ {
		bitmap.words[i] = bitmap.words[i] ^ bitmap2.words[i]
	}
}

// Clone returns a copy of the bitmap
// @return *Bitmap
func (bitmap *Bitmap) Clone() *Bitmap {
	words := make([]uint64, len(bitmap.words))
	copy(words, bitmap.words)
	return &Bitmap{words: words}
}

// 所有为1的Pos
func (bitmap *Bitmap) Pos1() []int {
	var pos []int
	for i, v := range bitmap.words {
		if v == 0 {
			continue
		}
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				pos = append(pos, 64*i+int(j))
			}
		}
	}
	return pos
}

// String returns the string representation of the bitmap
// @return string
func (bitmap *Bitmap) String() string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, v := range bitmap.words {
		if v == 0 {
			continue
		}
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				if buf.Len() > len("{") {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%d", 64*i+int(j))
			}
		}
	}
	buf.WriteByte('}')
	return buf.String()
}
