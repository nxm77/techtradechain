/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"strconv"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
)

// handleResult 处理结果，用于排序
type handleResult struct {
	nodeAddr    string
	blockHeight uint64
	elapsedTime int64
}

// newHandleResult create handleResult
func newHandleResult(nodeAddr string, blockHeight uint64, elapsedTime int64) *handleResult {
	return &handleResult{
		nodeAddr:    nodeAddr,
		blockHeight: blockHeight,
		elapsedTime: elapsedTime,
	}
}

type handleResultArray []*handleResult

// Len Swap Less 实现 sort.Interface 相关接口
func (a handleResultArray) Len() int      { return len(a) }
func (a handleResultArray) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a handleResultArray) Less(i, j int) bool {
	// 优先比较 blockHeight（降序）
	if a[i].blockHeight == a[j].blockHeight {
		// 当 blockHeight 相同时，比较 elapsedTime（升序）
		return a[i].elapsedTime < a[j].elapsedTime
	}
	return a[i].blockHeight > a[j].blockHeight
}

// CreateGetLastBlockTxRequest create tx request for get last block info
func (cc *ChainClient) CreateGetLastBlockTxRequest() (*common.TxRequest, error) {
	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_LAST_BLOCK.String(), []*common.KeyValuePair{
			{
				Key:   utils.KeyBlockContractWithRWSet,
				Value: []byte(strconv.FormatBool(false)),
			},
		}, defaultSeq, nil,
	)
	return cc.GenerateTxRequestWithPayer(payload, nil, nil)
}
