/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package helper

import (
	"techtradechain.com/techtradechain-go/module/core/common"
	commonpb "techtradechain.com/techtradechain/pb-go/v2/common"
	consensusPb "techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/protocol/v2"
	batch "techtradechain.com/techtradechain/txpool-batch/v2"
)

type maxBftHelper struct {
	txPool        protocol.TxPool
	chainConf     protocol.ChainConf
	proposalCache protocol.ProposalCache
	logger        protocol.Logger
}

func NewMaxbftHelper(txPool protocol.TxPool, chainConf protocol.ChainConf,
	proposalCache protocol.ProposalCache, log protocol.Logger) protocol.MaxbftHelper {
	return &maxBftHelper{
		txPool:        txPool,
		chainConf:     chainConf,
		proposalCache: proposalCache,
		logger:        log}
}

func (hp *maxBftHelper) DiscardBlocks(baseHeight uint64) {
	if hp.chainConf.ChainConfig().Consensus.Type != consensusPb.ConsensusType_MAXBFT {
		return
	}
	delBlocks := hp.proposalCache.DiscardBlocks(baseHeight)
	if len(delBlocks) == 0 {
		return
	}

	if common.TxPoolType == batch.TxPoolType {
		for _, delBlock := range delBlocks {
			batchIds, _, err := common.GetBatchIds(delBlock)
			if err != nil {
				// if get batch ids fail,discard other blocks.
				hp.logger.Warnf("get batch ids from block[%d,%x] failed, err:%v",
					delBlock.Header.BlockHeight, delBlock.Header.BlockHash, err)
				continue
			}
			hp.txPool.RetryAndRemoveTxBatches(batchIds, nil)
		}
		return
	}

	txs := make([]*commonpb.Transaction, 0, 100)
	for _, blk := range delBlocks {
		txs = append(txs, blk.Txs...)
	}

	common.RetryAndRemoveTxs(hp.txPool, txs, nil, hp.logger)
}
