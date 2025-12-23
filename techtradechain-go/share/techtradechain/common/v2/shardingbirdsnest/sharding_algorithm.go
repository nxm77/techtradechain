/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package shardingbirdsnest sharding algorithm
package shardingbirdsnest

import (
	bn "techtradechain.com/techtradechain/common/v2/birdsnest"
)

// ChecksumKeyModulo uint32 checksum
func ChecksumKeyModulo(key bn.Key, length int) int {
	// Take the last digit and find the modulus
	return int(key.Key()[key.Len()-1]) % length
}

// ModuloShardingAlgorithm sharding modulo algorithm
type ModuloShardingAlgorithm struct {
	// Length
	Length int
}

// NewModuloSA new sharding modulo algorithm
func NewModuloSA(l int) *ModuloShardingAlgorithm {
	return &ModuloShardingAlgorithm{Length: l}
}

// DoSharding 如果传入 shardingValues 小于 Length 则 最小设置为1
func (a ModuloShardingAlgorithm) DoSharding(shardingValues []bn.Key) [][]bn.Key {
	result := make([][]bn.Key, a.Length)
	// modulo sharding
	for i := range shardingValues {
		modulo := a.DoShardingOnce(shardingValues[i])
		if result[modulo] == nil {
			result[modulo] = []bn.Key{shardingValues[i]}
		} else {
			result[modulo] = append(result[modulo], shardingValues[i])
		}
	}
	return result
}

// DoShardingOnce do once sharding key
func (a ModuloShardingAlgorithm) DoShardingOnce(key bn.Key) (index int) {
	// last byte modulo
	return ChecksumKeyModulo(key, a.Length)
}
