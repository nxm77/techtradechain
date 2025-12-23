/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bloom

import (
	"fmt"
	"path"

	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
)

// BlockLoader used to load blocks
type BlockLoader interface {
	//Height get the last block height
	Height() uint64
	//GetBlockWithRWSets get block and all the rwsets corresponding to the block
	GetBlockWithRWSets(height uint64) (*storePb.BlockWithRWSet, error)
}

// ContractKeyGenerator generate a string with contract name and key
type ContractKeyGenerator func(contractName string, key []byte) []byte

// Options option config for bloom filter
type Options struct {
	dumpFile     string //持久化文件
	dumpCycle    int    //持久化周期
	parallelism  int
	syncAsync    bool        //异步同步增量数据
	dumpNonBlock bool        //非阻塞持久化(异步持久化)
	blockLoader  BlockLoader //区块加载器
	logger       protocol.Logger
	keyGenerator ContractKeyGenerator
}

// Option option function used to config options.
type Option func(*Options)

var defaultOptions = Options{
	dumpFile:    "bloom.dump",
	dumpCycle:   10,
	parallelism: 128,
}

// WithDumpFile set dump file for option
func WithDumpFile(file string) Option {
	return func(opt *Options) {
		opt.dumpFile = file
	}
}

// WithDumpCycle set dump cycle for option
func WithDumpCycle(cycle int) Option {
	return func(opt *Options) {
		opt.dumpCycle = cycle
	}
}

// WithRestoreAsync set restore async for option
func WithRestoreAsync() Option {
	return func(opt *Options) {
		opt.syncAsync = true
	}
}

// WithDumpAsync set dump async for option
func WithDumpAsync() Option {
	return func(opt *Options) {
		opt.dumpNonBlock = true
	}
}

// // WithParallelism set parallelism for add write sets in blocks to bloom filter
// func WithParallelism(parallelism int) Option {
// 	return func(opt *Options) {
// 		opt.parallelism = parallelism
// 	}
// }

// WithBlockLoader set block loader used to restore missed blocks when block bloom filter created.
func WithBlockLoader(loader BlockLoader) Option {
	return func(opt *Options) {
		opt.blockLoader = loader
	}
}

// WithLogger set logger used to print logger for block bloom filter
func WithLogger(logger protocol.Logger) Option {
	return func(o *Options) {
		o.logger = logger
	}
}

// WithContractKeyGenerator set contract key generator, block bloom filter use it to generate key for write set.
func WithContractKeyGenerator(generator ContractKeyGenerator) Option {
	return func(opt *Options) {
		opt.keyGenerator = generator
	}
}

// String print options
func (o *Options) String() string {
	return fmt.Sprintf("dump_file: %s, dump_cycle: %d, parallelism: %d, sync_async: %t, dump_non_block: %t",
		path.Base(o.dumpFile), o.dumpCycle, o.parallelism, o.syncAsync, o.dumpNonBlock)
}
