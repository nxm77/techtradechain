/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"context"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/txpool"
)

// GetPoolStatus Returns the max size of config transaction pool and common transaction pool,
// the num of config transaction in queue and pendingCache,
// and the the num of common transaction in queue and pendingCache.
func (cc *ChainClient) GetPoolStatus() (*txpool.TxPoolStatus, error) {
	cc.logger.Debug("[SDK] begin GetPoolStatus")
	client, err := cc.pool.getClient()
	if err != nil {
		return nil, err
	}
	req := &txpool.GetPoolStatusRequest{ChainId: cc.chainId}
	return client.rpcNode.GetPoolStatus(context.Background(), req)
}

// GetTxIdsByTypeAndStage Returns config or common txIds in different stage.
// txType may be TxType_CONFIG_TX, TxType_COMMON_TX, (TxType_CONFIG_TX|TxType_COMMON_TX)
// txStage may be TxStage_IN_QUEUE, TxStage_IN_PENDING, (TxStage_IN_QUEUE|TxStage_IN_PENDING)
func (cc *ChainClient) GetTxIdsByTypeAndStage(txType txpool.TxType, txStage txpool.TxStage) ([]string, error) {
	cc.logger.Debug("[SDK] begin GetTxIdsByTypeAndStage")
	client, err := cc.pool.getClient()
	if err != nil {
		return nil, err
	}
	req := &txpool.GetTxIdsByTypeAndStageRequest{
		ChainId: cc.chainId,
		TxType:  txType,
		TxStage: txStage,
	}
	resp, err := client.rpcNode.GetTxIdsByTypeAndStage(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return resp.TxIds, nil
}

// GetTxsInPoolByTxIds Retrieve the transactions by the txIds from the txPool,
// return transactions in the txPool and txIds not in txPool.
// default query upper limit is 1w transaction, and error is returned if the limit is exceeded.
func (cc *ChainClient) GetTxsInPoolByTxIds(txIds []string) ([]*common.Transaction, []string, error) {
	cc.logger.Debug("[SDK] begin GetTxsInPoolByTxIds")
	client, err := cc.pool.getClient()
	if err != nil {
		return nil, nil, err
	}
	req := &txpool.GetTxsInPoolByTxIdsRequest{
		ChainId: cc.chainId,
		TxIds:   txIds,
	}
	resp, err := client.rpcNode.GetTxsInPoolByTxIds(context.Background(), req)
	if err != nil {
		return nil, nil, err
	}
	return resp.Txs, resp.TxIds, nil
}
