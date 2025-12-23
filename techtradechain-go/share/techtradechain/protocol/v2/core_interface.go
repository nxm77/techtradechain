/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package protocol is a protocol package, which is base.
package protocol

import (
	"techtradechain.com/techtradechain/common/v2/msgbus"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	consensuspb "techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/pb-go/v2/consensus/maxbft"
	"techtradechain.com/techtradechain/pb-go/v2/txpool"
)

// DefaultBlockVersion default blockHeader.Version value
const DefaultBlockVersion = uint32(2030700) // default version of chain

// BlockCommitter put block and read write set into ledger(DB).
type BlockCommitter interface {
	// AddBlock Put block into ledger(DB) after block verify. Invoke by consensus or sync module.
	AddBlock(blk *common.Block) error
}

// BlockProposer generate new block when node is consensus proposer.
type BlockProposer interface {
	// Start proposer.
	Start() error
	// Stop proposer
	Stop() error
	// OnReceiveTxPoolSignal Receive propose signal from txpool module.
	OnReceiveTxPoolSignal(proposeSignal *txpool.TxPoolSignal)
	// OnReceiveProposeStatusChange Receive signal indicates if node is proposer from consensus module.
	OnReceiveProposeStatusChange(proposeStatus bool)
	// OnReceiveMaxBFTProposal Receive signal from maxbft consensus(Hotstuff) and propose new block.
	OnReceiveMaxBFTProposal(proposal *maxbft.BuildProposal)
	// ProposeBlock propose new block from maxbft consensus by sync call
	ProposeBlock(proposal *maxbft.BuildProposal) (*consensuspb.ProposalBlock, error)
	// OnReceiveRwSetVerifyFailTxs Receive signal from consensus and remove fails txs.
	OnReceiveRwSetVerifyFailTxs(rwSetVerifyFailTxs *consensuspb.RwSetVerifyFailTxs)
}

// BlockVerifier verify if a block is valid
type BlockVerifier interface {
	// Verify if a block is valid
	VerifyBlock(block *common.Block, mode VerifyMode) error
	VerifyBlockSync(block *common.Block, mode VerifyMode) (*consensuspb.VerifyResult, error)
	VerifyBlockWithRwSets(block *common.Block, rwsets []*common.TxRWSet, mode VerifyMode) error
}

// VerifyMode 区块验证模式
type VerifyMode int

const (
	//CONSENSUS_VERIFY 共识节点验证
	CONSENSUS_VERIFY VerifyMode = iota
	//SYNC_VERIFY 同步节点验证
	SYNC_VERIFY
	//PROPOSER_VERIFY 主节点验证
	PROPOSER_VERIFY
	//SYNC_FILTER_VERIFY 同步节点带交易过滤的验证
	SYNC_FILTER_VERIFY
)

// CoreEngine 核心引擎接口
type CoreEngine interface {
	Start()
	Stop()
	GetBlockProposer() BlockProposer
	GetBlockCommitter() BlockCommitter
	GetBlockVerifier() BlockVerifier
	msgbus.Subscriber
	//MaxbftHelper
	GetMaxbftHelper() MaxbftHelper
}

// StoreHelper 存储抽象接口
type StoreHelper interface {
	RollBack(*common.Block, BlockchainStore) error
	BeginDbTransaction(BlockchainStore, string)
	GetPoolCapacity() int
}
