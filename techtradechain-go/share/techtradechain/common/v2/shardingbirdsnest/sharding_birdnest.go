/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package shardingbirdsnest bird's nest
package shardingbirdsnest

import (
	"errors"
	"path/filepath"
	"time"

	"go.uber.org/atomic"

	bn "techtradechain.com/techtradechain/common/v2/birdsnest"
)

const (
	// Filepath sharding path
	Filepath = "sharding"
)

var (
	// ErrAddsTimeout adds timeout error
	ErrAddsTimeout = errors.New("add multiple key timeout")
	// ErrCannotModifyTheNestConfiguration cannot modify the nest configuration
	ErrCannotModifyTheNestConfiguration = errors.New("when historical data exists, you cannot modify the nest " +
		"configuration")
)

// ShardingBirdsNest Sharding bird's nest implement
type ShardingBirdsNest struct {
	// bn bird's nest collections
	bn []bn.BirdsNest
	// config sharding bird's nest configuration
	config *ShardingBirdsNestConfig
	// height current height
	height uint64
	// preHeight pre height
	preHeight *atomic.Uint64
	// sharding algorithm
	algorithm ShardingAlgorithm
	// log logger
	log bn.Logger
	// serializeC serialize channel
	serializeC chan serializeSignal
	// exitC exit channel TODO exit -> context.Context
	exitC chan struct{}
	// snapshot
	snapshot *bn.WalSnapshot
}

// NewShardingBirdsNest new sharding bird's nest implement
func NewShardingBirdsNest(config *ShardingBirdsNestConfig, exitC chan struct{}, strategy bn.Strategy,
	alg ShardingAlgorithm, logger bn.Logger) (*ShardingBirdsNest, error) {
	// eg: data/org1/tx_filter/chain1/sharding
	join := filepath.Join(config.Snapshot.Path, config.ChainId)
	snapshot, err := bn.NewWalSnapshot(join, Filepath, -1)
	if err != nil {
		return nil, err
	}
	// sharding bird's nest
	s := &ShardingBirdsNest{
		algorithm:  alg,
		exitC:      exitC,
		config:     config,
		snapshot:   snapshot,
		log:        logger,
		preHeight:  atomic.NewUint64(0),
		serializeC: make(chan serializeSignal),
	}
	// deserialize snapshot
	err = s.Deserialize()
	if err != nil {
		if err != ErrCannotModifyTheNestConfiguration {
			return nil, err
		}
	}
	// init bird's nest collections
	birdsNests := make([]bn.BirdsNest, config.Length)
	for i := 0; i < int(config.Length); i++ {
		var birdsNest bn.BirdsNest
		// new bird's nest by number
		birdsNest, err = bn.NewBirdsNestByNumber(config.Birdsnest, exitC, strategy, logger, i+1)
		if err != nil {
			if err != bn.ErrCannotModifyTheNestConfiguration {
				return nil, err
			}
		}
		birdsNests[i] = birdsNest
	}
	s.bn = birdsNests
	return s, err
}

// GetHeight get current height
func (s *ShardingBirdsNest) GetHeight() uint64 {
	// returned height
	return s.height
}

// SetHeight set current height
func (s *ShardingBirdsNest) SetHeight(height uint64) {
	s.height = height
	s.serializeHeight(height)
	// all bird's nest set current height
	for _, nest := range s.bn {
		nest.SetHeight(height)
	}
}

// AddsAndSetHeight Adds and SetHeight
func (s *ShardingBirdsNest) AddsAndSetHeight(keys []bn.Key, height uint64) (result error) {
	// add to sharding bird's nest
	err := s.Adds(keys)
	if err != nil {
		return err
	}
	s.SetHeight(height)
	return nil
}

// Adds add keys
func (s *ShardingBirdsNest) Adds(keys []bn.Key) (err error) {
	var (
		// sharding algorithm
		sharding = s.algorithm.DoSharding(keys)
		// finish channel
		finishC = make(chan int)
		// running task
		runningTask int
		// Timeout
		timeout = time.After(time.Duration(s.config.Timeout) * time.Second)
	)
	for i := 0; i < len(sharding); i++ {
		if sharding[i] == nil {
			continue
		}
		runningTask++
		// execute adds
		go func(i int, values []bn.Key) {
			defer func() { finishC <- i }()
			err = s.bn[i].Adds(values)
		}(i, sharding[i])
	}
	for {
		select {
		case <-timeout: // timeout
			return ErrAddsTimeout
		case <-finishC: // task finish
			if err != nil {
				return
			}
			runningTask--
			if runningTask <= 0 {
				// overall
				return
			}
		}
	}
}

// Add key
func (s *ShardingBirdsNest) Add(key bn.Key) error {
	if key == nil || key.Len() == 0 {
		return bn.ErrKeyCannotBeEmpty
	}
	// do sharding once
	index := s.algorithm.DoShardingOnce(key)
	// add to bird's nest
	err := s.bn[index].Add(key)
	if err != nil {
		return err
	}
	return nil
}

// Contains bn.Key
func (s *ShardingBirdsNest) Contains(key bn.Key, rules ...bn.RuleType) (bool, error) {
	// if the key is empty returned error
	if key == nil || key.Len() == 0 {
		return false, bn.ErrKeyCannotBeEmpty
	}
	// do sharding once
	index := s.algorithm.DoShardingOnce(key)
	// contains
	contains, err := s.bn[index].Contains(key, rules...)
	if err != nil {
		return false, err
	}
	return contains, nil
}

// ValidateRule validate key rule
func (s *ShardingBirdsNest) ValidateRule(key bn.Key, rules ...bn.RuleType) error {
	// if the key is empty returned error
	if key == nil || key.Len() == 0 {
		return bn.ErrKeyCannotBeEmpty
	}
	// TODO Although each Bird's Nest is independent, the rules are consistent, so there is no need for sharding and 0
	// is used by default; If Bird's Nest rules are inconsistent for each shard in the future, open the following code
	// index := s.algorithm.DoShardingOnce(key)
	// err := s.bn[index].ValidateRule(key, rules...)
	err := s.bn[0].ValidateRule(key, rules...)
	if err != nil {
		return err
	}
	return nil
}

// Info print sharding bird's nest info
func (s *ShardingBirdsNest) Info() []uint64 {
	// not implement
	return nil
}

// Infos print sharding bird's nest info
// index 0 sharding index
// index 0 height
// index 1 cuckoo size
// index 2 current index
// index 3 total cuckoo size
// index 4 total space occupied by cuckoo
func (s *ShardingBirdsNest) Infos() [][]uint64 {
	infos := make([][]uint64, s.config.Length)
	for i, birdsNest := range s.bn {
		infos[i] = birdsNest.Info()
	}
	return infos
}
