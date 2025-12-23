// Package bloom package
package bloom

/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"github.com/panjf2000/ants/v2"
)

type dumpFlag uint32

const (
	dumpFlagInProcess dumpFlag = 1 << iota
	dumpFlagPend
)

// BlockBloomFilter bloom filter for block
type BlockBloomFilter struct {
	filter         *BloomFilter
	opt            Options
	lastDumpHeight uint64
	synced         uint32
	dumpFlag       dumpFlag
	dumpLocker     sync.Mutex
	pool           *ants.Pool
	height         uint64
}

// NewBlockBloomFilter creates a new bloom filter.
// The param n is the number of items(keys) that the bloom filter will need to store.
// The param fp is the maximum false positive probability
// Uses the default options if no additional options are provided.
// Return an error if the bloom filter cannot be created.
func NewBlockBloomFilter(n uint, fp float64, opts ...Option) (*BlockBloomFilter, error) {
	opt := defaultOptions
	for _, o := range opts {
		o(&opt)
	}
	return NewBlockBloomFilterWithOption(n, fp, opt)
}

// NewBlockBloomFilterWithOption creates a new BlockBloomFilter object with the given Options.
// If the dumpFile in options exists, it loads the bloom filter from the file.
// Create a thread pool for concurrent addition of write sets in blocks.
// If a block loader is provided, it asynchronously or synchronously syncs the blocks.
// Note: the bloom filter will not provides the query function before syncs the blocks completely.
func NewBlockBloomFilterWithOption(n uint, fp float64, opt Options) (*BlockBloomFilter, error) {
	filter, err := newBloomFilter(n, fp, opt.dumpFile)
	if err != nil {
		return nil, err
	}
	pool, err := ants.NewPool(opt.parallelism)
	if err != nil {
		return nil, err
	}
	if opt.logger == nil {
		opt.logger = newLogger()
	}
	bFilter := &BlockBloomFilter{
		filter:         filter,
		opt:            opt,
		height:         filter.LastDumpLabel(),
		lastDumpHeight: filter.LastDumpLabel(),
		pool:           pool,
	}
	if opt.blockLoader != nil {
		if opt.syncAsync {
			go bFilter.syncBlocks(bFilter.height)
		} else {
			bFilter.syncBlocks(bFilter.height)
		}
	} else {
		bFilter.synced = 1
	}
	opt.logger.Infof("bloom filter {%d, %f} created at height[%d], %s", n, fp, bFilter.height, opt.String())
	if n != filter.ItemCapacity() || fp != filter.FP() {
		opt.logger.Infof("bloom filter {%d, %f} specified recover from file to {%d, %f}",
			n, fp, filter.ItemCapacity(), filter.FP())
	}
	return bFilter, nil
}

// AddBlock adds keys of all write sets in the block to the bloom filter.
func (f *BlockBloomFilter) AddBlock(block *storePb.BlockWithRWSet) {
	f.addBlock(block)
}

// AddBlockRWSets adds keys of all write sets to the bloom filter.
// The param height used as a mark when dumping data to  file.
func (f *BlockBloomFilter) AddBlockRWSets(height uint64, rwSets []*commonPb.TxRWSet) {
	f.addBlockRWSets(height, rwSets)
}

// Has test a key exists in the bloom filter or not.
// It doesn't work and return true always if the bloom filter is restoring data.
func (f *BlockBloomFilter) Has(contractName string, key []byte) bool {
	if !f.hasSynced() {
		return true
	}
	if f.opt.keyGenerator != nil {
		return f.has(f.opt.keyGenerator(contractName, key))
	}
	return f.has(key)
}

func (f *BlockBloomFilter) has(k []byte) bool {
	return f.filter.Test(k)
}

// Add adds a key to the bloom filter.
func (f *BlockBloomFilter) Add(k []byte) {
	f.filter.Add(k)
}

// Dump dumping manually is not allowed before the filter has finished syncing,
// because this behavior can lead to errors in the restore data process.
// If allowed, it will dump bloom filter data to file with current height.
func (f *BlockBloomFilter) Dump() error {
	if f.hasSynced() {
		return f.dumpSync(f.height)
	}
	return fmt.Errorf("cant dump manually before sync complete")
}

// LastDumpHeight returns the last dump height
func (f *BlockBloomFilter) LastDumpHeight() uint64 {
	return f.lastDumpHeight
}

func (f *BlockBloomFilter) addBlock(block *storePb.BlockWithRWSet) bool {
	if block == nil || block.Block == nil {
		return false
	}
	f.addBlockRWSets(block.Block.Header.BlockHeight, block.TxRWSets)
	return true
}

// addBlockRWSets adds keys of all write sets to the bloom filter.
// if the specified dumpCycle is gather than 0, and
// the difference between the current height and the height of the last dump exceeds the dumpCycle,
// trigger do a dump.
func (f *BlockBloomFilter) addBlockRWSets(h uint64, rwSets []*commonPb.TxRWSet) {
	f.addRWSets(rwSets)
	f.height = h
	if f.opt.dumpCycle > 0 && f.canDump(f.height) {
		if err := f.dump(); err != nil {
			f.opt.logger.Warnf("dump bloom filter error after add block[%d]: %s", f.height, err)
		}
	}
}

// addRWSets write the write sets of each transaction to the filter concurrently.
func (f *BlockBloomFilter) addRWSets(rwSets []*commonPb.TxRWSet) {
	wg := sync.WaitGroup{}
	for i := range rwSets {
		func(i int) {
			if len(rwSets[i].TxWrites) > 0 {
				wg.Add(1)
				if err := f.pool.Submit(func() {
					defer wg.Done()
					f.addBlockWriteSets(rwSets[i].TxWrites)
				}); err != nil {
					f.opt.logger.Warnf("failed to submit add write sets task to pool: %s", err)
				}
			}
		}(i)
	}
	wg.Wait()
}

func (f *BlockBloomFilter) needDump(h uint64) bool {
	return f.opt.dumpCycle > 0 && int(h-f.lastDumpHeight) >= f.opt.dumpCycle
}

func (f *BlockBloomFilter) canDump(h uint64) bool {
	return f.needDump(h) && f.hasSynced()
}

func (f *BlockBloomFilter) hasSynced() bool {
	return atomic.LoadUint32(&f.synced) == 1
}

func (f *BlockBloomFilter) addBlockWriteSets(wSets []*commonPb.TxWrite) {
	for _, wSet := range wSets {
		if f.opt.keyGenerator != nil {
			f.Add(f.opt.keyGenerator(wSet.ContractName, wSet.Key))
			continue
		}
		f.Add(wSet.Key)
	}
}

func (f *BlockBloomFilter) dump() error {
	if f.opt.dumpNonBlock {
		f.dumpAsync()
		return nil
	}
	return f.dumpSync(f.height)
}

// dumpSync if nobody is dumping it will set the flag with dumpFlagInProcess
// (indicates that a dump operation is in progress) and perform dump operation.
// Dumping successfully updates the last dump height to h.
func (f *BlockBloomFilter) dumpSync(h uint64) error {
	err := ErrDumpBusy
	f.dumpLocker.Lock()
	if f.dumpFlag&dumpFlagInProcess == 0 {
		f.dumpFlag |= dumpFlagInProcess
		lastDumpHeight := f.lastDumpHeight
		f.dumpLocker.Unlock()
		start := time.Now()
		err = f.filter.Dump(h)
		if err == nil {
			f.opt.logger.Infof("dump bloom filter at[%d] use %dms", h, time.Since(start).Milliseconds())
			lastDumpHeight = h
		} else {
			f.opt.logger.Warnf("dump bloom filter at[%d] failed: %s", h, err)
		}
		f.dumpLocker.Lock()
		f.dumpFlag &= ^dumpFlagInProcess
		f.lastDumpHeight = lastDumpHeight
	}
	f.dumpLocker.Unlock()
	return err
}

// // copyAndDump copies the bloom filter and dump it to file.
// func (f *BlockBloomFilter) copyAndDump(h uint64) error {
// 	dupFilter := f.filter.Copy()
// 	if err := dupFilter.Dump(h); err != nil {
// 		f.opt.logger.Warnf("failed to dump copied bloom filter at[%d]: %s", h, err)
// 		return err
// 	}
// 	f.opt.logger.Debugf("dump copied bloom filter at[%d]", h)
// 	return nil
// }

// 如果有在执行dump的协程，则只设置一个pend标识，正在执行dump的协程在dump完成后，会check这个pend标识，如果有，继续dump。
// 1. 尽量将所有的异步dump放到一个go routine执行，减少非必要的go routine开销
// 2. 在异步dump的过程中避免错过触发信号
// 每次调用dumpAsync，都会更新lastDumpHeight，以防止频繁地触发dump，如果dump失败，则回退lastDumpHeight。
func (f *BlockBloomFilter) dumpAsync() {
	f.dumpLocker.Lock()
	f.dumpFlag |= dumpFlagPend
	if f.dumpFlag&dumpFlagInProcess == 0 {
		f.dumpFlag |= dumpFlagInProcess
		lastDumpHeight := f.lastDumpHeight
		go func() {
			var err error
			for {
				f.dumpLocker.Lock()
				if f.dumpFlag&dumpFlagPend == 0 || err != nil {
					break
				}
				f.dumpFlag &= ^dumpFlagPend
				f.dumpLocker.Unlock()
				h := f.height
				start := time.Now()
				// err = f.copyAndDump(h)
				err = f.filter.Dump(h)
				f.opt.logger.Infof("async dump bloom filter at[%d] use %d ms", h, time.Since(start).Milliseconds())
				if err != nil {
					f.opt.logger.Warnf("failed to async dump bloom filter at[%d]: %s", h, err)
					continue
				}
				lastDumpHeight = h
			}
			f.dumpFlag &= ^dumpFlagInProcess
			f.lastDumpHeight = lastDumpHeight
			f.dumpLocker.Unlock()
		}()
	}
	f.lastDumpHeight = f.height //暂存当前触发dump的高度，以避免dump过程中一直触发dump信号
	f.dumpLocker.Unlock()
}

// syncBlocks will sync all blocks from last dump height to current height.
// Do dump if needed.
func (f *BlockBloomFilter) syncBlocks(h uint64) {
	defer func() {
		atomic.StoreUint32(&f.synced, 1)
		f.opt.logger.Infof("bloom filter end restore to [%d]", h)
	}()
	loader := f.opt.blockLoader
	if loader == nil {
		return
	}

	targetHeight := loader.Height()
	logPerProcessMask := uint64(63)
	defer func(sub bool) {
		if sub {
			h--
		}
	}(h <= targetHeight)
	f.opt.logger.Infof("bloom filter start restore blocks from[%d] to [%d]", h, targetHeight)
	for ; h <= targetHeight; h++ {
		block, err := loader.GetBlockWithRWSets(h)
		if err != nil {
			f.opt.logger.Errorf("failed to restore block[%d]: %s", h, err)
			h--
			continue
		}
		f.addBlock(block)
		if f.needDump(h) {
			if err := f.dumpSync(h); err != nil {
				f.opt.logger.Warnf("failed to dump filter when restore block[%d]: %s", h, err)
			}
		}
		if (h & logPerProcessMask) == 0 {
			f.opt.logger.Debugf("bloom filter restored blocks to [%d]", h)
		}
	}
}

// Close release the goroutines pool and close the bloom filter
func (f *BlockBloomFilter) Close() error {
	f.pool.Release()
	return f.filter.Close()
}
