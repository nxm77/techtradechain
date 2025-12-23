/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package cache

// ringBuffer is a non-thread-safe ring buffer
type ringBuffer struct {
	data  []uint64
	w, r  int
	cap   int
	count int
}

// NewRingBuffer ceates a new ring buffer with the specified capacity
func newRingBuffer(cap int) *ringBuffer {
	return &ringBuffer{
		data: make([]uint64, cap),
		cap:  cap,
	}
}

// Push add an element to the ring buffer.
// If the buffer is full, it returns false and does not add the element.
func (rb *ringBuffer) push(value uint64) bool {
	if rb.count == rb.cap {
		return false
	}

	rb.data[rb.w] = value
	rb.w = (rb.w + 1) % rb.cap
	rb.count++
	return true
}

// Pop removes and returns the oldest element from the ring buffer.
// If the buffer is empty, it returns false and does not remove any element.
func (rb *ringBuffer) pop() (uint64, bool) {
	if rb.count == 0 {
		return 0, false
	}

	value := rb.data[rb.r]
	rb.r = (rb.r + 1) % rb.cap
	rb.count--
	return value, true
}

// Peek returns the i-th oldest element in the ring buffer without removing it.
// If the index is out of bounds, it returns false.
func (rb *ringBuffer) peek(i int) (uint64, bool) {
	if i < 0 || i >= rb.count {
		return 0, false
	}
	index := (rb.r + i) % rb.cap
	return rb.data[index], true
}

// resize resize the ring buffer to the given capacity.
func (rb *ringBuffer) resize(cap int) bool {
	if cap < rb.count || cap == rb.cap {
		return false
	}
	newData := make([]uint64, cap)
	for i := 0; i < rb.count; i++ {
		newData[i] = rb.data[(rb.r+i)%rb.cap]
	}
	rb.data = newData
	rb.cap = cap
	rb.r = 0
	rb.w = rb.count
	return true
}

// size returns the number of elements in the ring buffer.
func (rb *ringBuffer) size() int {
	return rb.count
}

// capacity returns the capacity of the ring buffer.
func (rb *ringBuffer) capacity() int {
	return rb.cap
}
