/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package shardingbirdsnest sharding bird's nest configuration
package shardingbirdsnest

import bn "techtradechain.com/techtradechain/common/v2/birdsnest"

// ShardingBirdsNestConfig Sharding bird's Nest configuration
type ShardingBirdsNestConfig struct {
	// ChainId
	ChainId string `json:"chain_id,omitempty"`
	// Length bird's nest numbers
	Length uint32 `json:"length,omitempty"`
	// Timeout sharding task timeout
	Timeout int64 `json:"timeout,omitempty"`
	// Birdsnest Bird's Nest configuration
	Birdsnest *bn.BirdsNestConfig `json:"birdsnest,omitempty"`
	// Snapshot configuration
	Snapshot *bn.SnapshotSerializerConfig `json:"snapshot,omitempty"`
}
