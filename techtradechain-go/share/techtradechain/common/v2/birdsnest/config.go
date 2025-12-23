/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest configuration implement
package birdsnest

// KeyType key type
type KeyType int32

const (
	// KeyType_KTDefault default type
	KeyType_KTDefault KeyType = 0
	// KeyType_KTTimestampKey timestamp type
	KeyType_KTTimestampKey KeyType = 1
)

// KeyType_name key type name map
var KeyType_name = map[KeyType]string{
	KeyType_KTDefault:      "Default",
	KeyType_KTTimestampKey: "TimestampKey",
}

// SerializeIntervalType Serialize interval type
type SerializeIntervalType int32

const (
	// SerializeIntervalType_Height Timed serialize type
	SerializeIntervalType_Height SerializeIntervalType = 0
	// SerializeIntervalType_Timed Timed serialize type
	SerializeIntervalType_Timed SerializeIntervalType = 1
	// SerializeIntervalType_Exit  Exit serialize type
	SerializeIntervalType_Exit SerializeIntervalType = 2
)

// SerializeIntervalType_name SerializeIntervalType name map
var SerializeIntervalType_name = map[SerializeIntervalType]string{
	SerializeIntervalType_Height: "Height",
	SerializeIntervalType_Timed:  "Timed",
	SerializeIntervalType_Exit:   "Exit",
}

// FilterExtensionType filter extension type
type FilterExtensionType int32

const (
	// FilterExtensionType_FETDefault default filter extension type
	FilterExtensionType_FETDefault FilterExtensionType = 0
	// FilterExtensionType_FETTimestamp timestamp filter extension type
	FilterExtensionType_FETTimestamp FilterExtensionType = 1
)

// RuleType rule type
type RuleType int32

const (
	// RuleType_AbsoluteExpireTime absolute expire time
	RuleType_AbsoluteExpireTime RuleType = 0
)

// BirdsNestConfig bird's nest configuration
type BirdsNestConfig struct {
	// ChainId
	ChainId string `json:"chain_id,omitempty"`
	// Length cuckoo numbers
	Length uint32 `json:"length,omitempty"`
	// rules configuration
	Rules *RulesConfig `json:"rules,omitempty"`
	// Cuckoo configuration
	Cuckoo *CuckooConfig `json:"cuckoo,omitempty"`
	// Snapshot configuration
	Snapshot *SnapshotSerializerConfig `json:"snapshot,omitempty"`
}

// RulesConfig rules configuration
type RulesConfig struct {
	// absolute expire time second
	AbsoluteExpireTime int64 `json:"absolute_expire_time,omitempty"`
}

// CuckooConfig Cuckoo configuration
type CuckooConfig struct {
	// KeyType key type
	KeyType KeyType `json:"key_type,omitempty"`
	// TagsPerBucket num of tags for each bucket, which is b in paper. tag is fingerprint, which is f in paper.
	TagsPerBucket uint32 `json:"tags_per_bucket,omitempty"`
	// BitsPerItem num of bits for each item, which is length of tag(fingerprint)
	BitsPerItem uint32 `json:"bits_per_item,omitempty"`
	// MaxNumKeys num of keys that filter will store. this value should close to and lower
	//					 nextPow2(maxNumKeys/tagsPerBucket) * maxLoadFactor. cause table.NumBuckets is always a
	//					 power of two
	MaxNumKeys uint32 `json:"max_num_keys,omitempty"`
	// TableType has two constant parameters to choose from:
	// TableTypeSingle normal single table
	// TableTypePacked packed table, use semi-sort to save 1 bit per item
	TableType uint32 `json:"table_type,omitempty"`
}

// SnapshotSerializerConfig Snapshot serializer config
type SnapshotSerializerConfig struct {
	// Type serialize interval type
	Type SerializeIntervalType `json:"type,omitempty"`
	// Timed serialize interval configuration
	Timed *TimedSerializeIntervalConfig `json:"timed,omitempty"`
	// BlockHeight serialize interval configuration
	BlockHeight *BlockHeightSerializeIntervalConfig `json:"block_height,omitempty"`
	// Path filepath
	Path string `json:"path,omitempty"`
}

// TimedSerializeIntervalConfig Timed serialization interval configuration
type TimedSerializeIntervalConfig struct {
	// Timed Interval
	Interval int64 `json:"interval,omitempty"`
}

// BlockHeightSerializeIntervalConfig Block height serialization interval configuration
type BlockHeightSerializeIntervalConfig struct {
	// Block height Interval
	Interval uint64 `json:"interval,omitempty"`
}
