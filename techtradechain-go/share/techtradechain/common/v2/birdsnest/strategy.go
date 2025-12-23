/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest strategy
package birdsnest

// Strategy function
type Strategy func(bn *BirdsNestImpl) error

// LruStrategy Nest filter cycle elimination strategy
func LruStrategy(bn *BirdsNestImpl) error {
	i := seeNextIndex(bn.currentIndex, int(bn.config.Length+1))
	bn.filters[i] = NewCuckooFilter(bn.config.Cuckoo)
	bn.log.Debugf("filter %v is full, filter %v eliminate success", bn.currentIndex, i)
	bn.currentIndex = i
	return nil
}

// see next index and currentIndex reset
func seeNextIndex(index, both int) int {
	index++
	return index % both
}
