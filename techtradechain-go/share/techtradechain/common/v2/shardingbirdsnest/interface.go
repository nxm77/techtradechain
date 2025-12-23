/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package shardingbirdsnest interface
package shardingbirdsnest

import bn "techtradechain.com/techtradechain/common/v2/birdsnest"

// ShardingAlgorithm sharding algorithm
type ShardingAlgorithm interface {
	// DoSharding do sharding
	DoSharding(shardingValues []bn.Key) [][]bn.Key
	// DoShardingOnce once sharding
	DoShardingOnce(bn.Key) (index int)
}

// KeyModuloAlgorithm key modulo algorithm
type KeyModuloAlgorithm func(key bn.Key, length int) int
