/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest cuckoo
package birdsnest

import (
	"encoding/json"
	"math"

	birdsnestpb "techtradechain.com/techtradechain/common/v2/birdsnest/pb"

	"github.com/linvon/cuckoo-filter"
)

var (
	// The load factor
	loadFactorMap map[uint32]float64
)

const (
	// DefaultLoadFactor Default load factor
	DefaultLoadFactor = 0.98
)

// init Initialize the load factor
func init() {
	loadFactorMap = make(map[uint32]float64)
	// 大小 b=2、4 或 8 时则分别会增加到 84%、95% 和 98%
	loadFactorMap[2] = 0.84
	loadFactorMap[4] = 0.95
	loadFactorMap[8] = DefaultLoadFactor
}

// CuckooFilterImpl Cuckoo Filter
type CuckooFilterImpl struct {
	// cuckoo filter
	cuckoo cuckoo.Filter
	// filter extension
	extension FilterExtension
	// cuckoo config
	config *CuckooConfig
	// cuckoo is full
	full bool
}

// newCuckooFilters Create multiple CuckooFilter
func newCuckooFilters(config *CuckooConfig, size uint32) []CuckooFilter {
	filters := make([]CuckooFilter, size)
	for i := uint32(0); i < size; i++ {
		// New cuckoo filter
		filters[i] = NewCuckooFilter(config)
	}
	return filters
}

// newCuckooFiltersByDecode New cuckoo filters by decode
func newCuckooFiltersByDecode(filters []*birdsnestpb.CuckooFilter) ([]CuckooFilter, error) {
	filters0 := make([]CuckooFilter, len(filters))
	for i := 0; i < len(filters); i++ {
		filter, err := NewCuckooFilterByDecode(filters[i])
		if err != nil {
			return nil, err
		}
		filters0[i] = filter
		if filter.IsFull() {
			continue
		}
		if filter.cuckoo.Size() >= uint(filter.config.MaxNumKeys) {
			filter.full = true
		}
	}
	return filters0, nil
}

// NewCuckooFilter new cuckoo filter
// Params:
// common.CuckooConfig.TableType    : has two constant parameters to choose from:
// 1. TableTypeSingle normal single table
// 2. TableTypePacked packed table, use semi-sort to save 1 bit per item
// common.CuckooConfig.TagsPerBucket: num of tags for each bucket, which is b in paper. tag is fingerprint, which is f
// in paper.
// common.CuckooConfig.MaxNumKeys   : num of keys that filter will store. this value should close to and lower
// nextPow2(maxNumKeys/tagsPerBucket) * maxLoadFactor. cause table.NumBuckets is always a power of two
// common.CuckooConfig.BitsPerItem  : num of bits for each item, which is length of tag(fingerprint)
// common.CuckooConfig.TableType    :
// common.CuckooConfig.KeyType      :
// 0 TableTypeSingle normal single table
// 1 TableTypePacked packed table, use semi-sort to save 1 bit per item 1 is recommended
// Result: CuckooFilter
func NewCuckooFilter(config *CuckooConfig) CuckooFilter {
	extensionType := statusConvertExtension(config.KeyType)
	if extensionType == -1 {
		return nil
	}
	extension, err := Factory().New(extensionType)
	if err != nil {
		return nil
	}
	// maxNumKeys := getApproximationMaxNumKeys(config.MaxNumKeys, config.MaxNumKeys)
	return &CuckooFilterImpl{
		cuckoo: *cuckoo.NewFilter(uint(config.TagsPerBucket), uint(config.BitsPerItem),
			getApproximationMaxNumKeys(config.MaxNumKeys, config.TagsPerBucket),
			uint(config.TableType)),
		extension: extension,
		config:    config,
	}
}

// NewCuckooFilterByDecode new cuckoo filter by decode
func NewCuckooFilterByDecode(filter *birdsnestpb.CuckooFilter) (*CuckooFilterImpl, error) {
	decode, err := cuckoo.Decode(filter.Cuckoo)
	if err != nil {
		return nil, err
	}
	extension, err := ExtensionDeserialize(filter.Extension)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	var config CuckooConfig
	err = json.Unmarshal(filter.Config, &config)
	if err != nil {
		return nil, err
	}
	return &CuckooFilterImpl{
		cuckoo:    *decode,
		extension: extension,
		config:    &config,
	}, nil
}

// Extension cuckoo filter
func (c *CuckooFilterImpl) Extension() FilterExtension {
	return c.extension
}

// IsFull is full
func (c *CuckooFilterImpl) IsFull() bool {
	return c.full
}

// Add key to cuckoo filter
func (c *CuckooFilterImpl) Add(key Key) (bool, error) {
	add := c.cuckoo.Add(key.Key())
	if !add {
		// The cuckoo filter is full if it is not added successfully
		c.full = true
		return false, nil
	}
	if c.cuckoo.Size() >= uint(c.config.MaxNumKeys) {
		// If the size of the cuckoo filter is greater than or equal to the configured size, the filter is full
		c.full = true
	}
	//
	err := c.extension.Store(key)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Contains Whether the cuckoo filter contains keys
func (c *CuckooFilterImpl) Contains(key Key) (bool, error) {
	err := c.extension.Validate(key, c.IsFull())
	if err != nil {
		if err == ErrKeyTimeIsNotInTheFilterRange {
			// Not in the time interval
			return false, nil
		}
		return false, err
	}
	return c.cuckoo.Contain(key.Key()), nil
}

// Encode cuckoo filter
func (c *CuckooFilterImpl) Encode() (FilterEncoder, error) {
	encode, err := c.cuckoo.Encode()
	if err != nil {
		return FilterEncoder{}, err
	}
	config, err := json.Marshal(c.config)
	if err != nil {
		return FilterEncoder{}, err
	}

	return newFilterEncoder(encode, config, c.full), nil
}

// Config config
func (c *CuckooFilterImpl) Config() ([]byte, error) {
	return c.cuckoo.Encode()
}

// Info info
// index 0 cuckoo size
// index 1 Space occupied by cuckoo
func (c *CuckooFilterImpl) Info() []uint64 {
	var info = make([]uint64, 2)
	info[0] = uint64(c.cuckoo.Size())
	info[1] = uint64(c.cuckoo.SizeInBytes())
	return info
}

// FilterEncoder filter encoder
type FilterEncoder struct {
	// filter
	filter []byte
	// config
	config []byte
	// full
	full bool
}

func newFilterEncoder(filter []byte, config []byte, full bool) FilterEncoder {
	return FilterEncoder{filter: filter, config: config, full: full}
}

func getApproximationMaxNumKeys(maxNumKeys, b uint32) uint {
	loadFactor, ok := loadFactorMap[b]
	if !ok {
		loadFactor = DefaultLoadFactor
	}
	got := float64(maxNumKeys) * 1.25 / loadFactor
	for i := float64(1); true; i++ {
		pow := math.Pow(2, i)
		rl := pow * loadFactor
		if rl > got {
			return uint(rl)
		}
	}
	return uint(maxNumKeys)
}
