/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package blockcontract

import (
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/utils/v2/cache"
)

// checkRoleAndFilterBlockTxs 检查用户有没有查询的权限
// @param block
// @param txSimContext
// @return *commonPb.Block
// @return error
func checkRoleAndFilterBlockTxs(
	block *commonPb.Block,
	txSimContext protocol.TxSimContext,
	txRWSet []*commonPb.TxRWSet,
	events []*commonPb.ContractEvent) (
	*commonPb.Block,
	[]*commonPb.TxRWSet,
	[]*commonPb.ContractEvent,
	error) {
	var (
		reqSender protocol.Role
		err       error
		ac        protocol.AccessControlProvider
	)

	ac, err = txSimContext.GetAccessControl()
	if err != nil {
		return block, txRWSet, events, err
	}

	reqSender, err = utils.GetRoleFromTx(txSimContext.GetTx(), ac)
	if err != nil {
		return block, txRWSet, events, err
	}

	// 交易id在黑名单无权查看
	newBlock := utils.FilterBlockBlacklistTxs(block)
	newEvents := utils.FilterBlockBlacklistEvents(events, block.Header.ChainId)
	newTxRWSet := utils.FilterBlockBlacklistTxRWSet(txRWSet, block.Header.ChainId)

	// light 用户过滤交易
	reqSenderOrgId := txSimContext.GetTx().Sender.Signer.OrgId
	if reqSender == protocol.RoleLight {
		newBlock = utils.FilterBlockTxs(reqSenderOrgId, newBlock)
	}

	return newBlock, newTxRWSet, newEvents, nil
}

// checkRoleAndGenerateTransactionInfo 验证是否有查询Tx的权限
// @param txSimContext
// @param transactionInfo
// @return *commonPb.TransactionInfoWithRWSet
// @return error
func checkRoleAndGenerateTransactionInfo(txSimContext protocol.TxSimContext,
	transactionInfo *commonPb.TransactionInfoWithRWSet) (*commonPb.TransactionInfoWithRWSet, error) {
	var (
		reqSender protocol.Role
		err       error
		ac        protocol.AccessControlProvider
	)
	tx := transactionInfo.Transaction

	if ac, err = txSimContext.GetAccessControl(); err != nil {
		return nil, err
	}

	if reqSender, err = utils.GetRoleFromTx(txSimContext.GetTx(), ac); err != nil {
		return nil, err
	}

	var newTransactionInfo = &commonPb.TransactionInfoWithRWSet{
		Transaction:    transactionInfo.Transaction,
		BlockHeight:    transactionInfo.BlockHeight,
		BlockHash:      transactionInfo.BlockHash,
		TxIndex:        transactionInfo.TxIndex,
		BlockTimestamp: transactionInfo.BlockTimestamp,
		RwSet:          transactionInfo.RwSet,
	}
	// 验证轻节点无权查看
	if reqSender == protocol.RoleLight {
		if tx.Sender.Signer.OrgId != txSimContext.GetTx().Sender.Signer.OrgId {
			newTransactionInfo = &commonPb.TransactionInfoWithRWSet{
				Transaction:    nil,
				BlockHeight:    transactionInfo.BlockHeight,
				BlockHash:      transactionInfo.BlockHash,
				TxIndex:        transactionInfo.TxIndex,
				RwSet:          nil,
				BlockTimestamp: transactionInfo.BlockTimestamp,
			}
		}
	}

	// 交易id在黑名单无权查看
	bl := cache.NewCacheList(utils.NativePrefix + tx.Payload.ChainId)
	if bl.Exists(tx.Payload.TxId) {
		contractResult := transactionInfo.Transaction.Result.ContractResult
		if contractResult == nil {
			contractResult = &commonPb.ContractResult{}
		}
		txTmp := &commonPb.Transaction{
			Payload: &commonPb.Payload{
				ChainId:        transactionInfo.Transaction.Payload.ChainId,
				TxType:         transactionInfo.Transaction.Payload.TxType,
				TxId:           transactionInfo.Transaction.Payload.TxId,
				Timestamp:      transactionInfo.Transaction.Payload.Timestamp,
				ExpirationTime: transactionInfo.Transaction.Payload.ExpirationTime,
				ContractName:   utils.Violation,
				Method:         utils.Violation,
				Parameters:     nil,
				Sequence:       transactionInfo.Transaction.Payload.Sequence,
				Limit:          transactionInfo.Transaction.Payload.Limit,
			},
			Sender:    transactionInfo.Transaction.Sender,
			Endorsers: transactionInfo.Transaction.Endorsers,
			Result: &commonPb.Result{
				Code: transactionInfo.Transaction.Result.Code,
				ContractResult: &commonPb.ContractResult{
					Code:          contractResult.Code,
					Result:        nil,
					Message:       utils.Violation,
					GasUsed:       contractResult.GasUsed,
					ContractEvent: nil,
				},
				RwSetHash: transactionInfo.Transaction.Result.RwSetHash,
				Message:   utils.Violation,
			},
			Payer: transactionInfo.Transaction.Payer,
		}
		newTransactionInfo = &commonPb.TransactionInfoWithRWSet{
			Transaction:    txTmp,
			BlockHeight:    transactionInfo.BlockHeight,
			BlockHash:      transactionInfo.BlockHash,
			TxIndex:        transactionInfo.TxIndex,
			RwSet:          nil,
			BlockTimestamp: transactionInfo.BlockTimestamp,
		}
		return newTransactionInfo, nil
	}

	return newTransactionInfo, nil
}
