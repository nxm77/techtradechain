/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/discovery"
	"techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	"github.com/gogo/protobuf/proto"
)

func (cc *ChainClient) queryTxInfo(parameter ...parameterBuilder) (
	*common.TransactionInfoWithRWSet, error) {
	var parameters []*common.KeyValuePair
	for _, p := range parameter {
		parameters = append(parameters, p())
	}
	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_TX_BY_TX_ID.String(), parameters, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		if utils.IsArchived(resp.Code) {
			return nil, errors.New(resp.Code.String())
		}
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	tx := &common.TransactionInfoWithRWSet{}
	if err = proto.Unmarshal(resp.ContractResult.Result, tx); err != nil {
		return nil, fmt.Errorf("GetTxByTxId unmarshal failed, %s", err)
	}

	return tx, nil
}

// GetTxByTxId get tx by tx id, returns *common.TransactionInfo
func (cc *ChainClient) GetTxByTxId(txId string) (*common.TransactionInfo, error) {
	// 首先去归档中心查询
	if cc.IsArchiveCenterQueryFist() {
		tx, err := cc.GetArchiveService().GetTxByTxId(txId)
		if err == nil && tx != nil {
			return tx, nil
		}
	}
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[txId:%s]",
		syscontract.ChainQueryFunction_GET_TX_BY_TX_ID, txId)
	tx, err := cc.queryTxInfo(parTxId(txId))
	if err != nil {
		return nil, err
	}
	transactionInfo := &common.TransactionInfo{
		Transaction:    tx.Transaction,
		BlockHeight:    tx.BlockHeight,
		BlockHash:      tx.BlockHash,
		TxIndex:        tx.TxIndex,
		BlockTimestamp: tx.BlockTimestamp,
	}
	return transactionInfo, nil
}

// GetTxWithRWSetByTxId get tx with rwset by tx id
func (cc *ChainClient) GetTxWithRWSetByTxId(txId string) (*common.TransactionInfoWithRWSet, error) {
	// 首先去归档中心查询
	if cc.IsArchiveCenterQueryFist() {
		tx, err := cc.GetArchiveService().GetTxWithRWSetByTxId(txId)
		if err == nil && tx != nil {
			return tx, nil
		}
	}
	cc.logger.Debugf("[SDK] begin GetTxWithRWSetByTxId, [method:%s]/[txId:%s]",
		syscontract.ChainQueryFunction_GET_TX_BY_TX_ID, txId)
	return cc.queryTxInfo(parTxId(txId), parWithRWSet(true))
}

// GetTxByTxIdTruncate 根据TxId获得Transaction对象，并根据参数进行截断
// @param txId
// @param withRWSet
// @param truncateLength
// @param truncateModel
// @return *common.TransactionInfoWithRWSet
// @return error
func (cc *ChainClient) GetTxByTxIdTruncate(txId string, withRWSet bool, truncateLength int,
	truncateModel string) (*common.TransactionInfoWithRWSet, error) {
	// 首先去归档中心查询
	if cc.IsArchiveCenterQueryFist() {
		tx, err := cc.GetArchiveService().GetTxWithRWSetByTxId(txId)
		if err == nil && tx != nil {
			return tx, nil
		}
		//TODO 归档中心的情况下，支持截断？
	}

	cc.logger.Debugf("[SDK] begin GetTxWithRWSetByTxId, [method:%s]/[txId:%s]",
		syscontract.ChainQueryFunction_GET_TX_BY_TX_ID, txId)
	return cc.queryTxInfo(parTxId(txId), parWithRWSet(withRWSet), parTruncateValueLen(truncateLength),
		parTruncateModelReplace(truncateModel))
}

type parameterBuilder func() *common.KeyValuePair

func parBlockHeight(blockHeight uint64) parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractBlockHeight,
			Value: []byte(strconv.FormatUint(blockHeight, 10)),
		}
	}
}
func parWithRWSet(withRWSet bool) parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractWithRWSet,
			Value: []byte(strconv.FormatBool(withRWSet)),
		}
	}
}
func parBlockHash(blockHash string) parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractBlockHash,
			Value: []byte(blockHash),
		}
	}
}

func parTxId(txId string) parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractTxId,
			Value: []byte(txId),
		}
	}
}

func parTruncateValueLen(length int) parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractTruncateValueLen,
			Value: []byte(strconv.Itoa(length)),
		}
	}
}

// parTruncateModelEmpty 超出长度则清空Value
// @return parameterBuilder
func parTruncateModelEmpty() parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractTruncateModel,
			Value: []byte("empty"),
		}
	}
}

// parTruncateModelTruncate 超出长度则截断超出部分的Value
// @return parameterBuilder
func parTruncateModelTruncate() parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractTruncateModel,
			Value: []byte("truncate"),
		}
	}
}

// parTruncateModelHash 超出长度则计算Value的Hash替代Value
// @return parameterBuilder
func parTruncateModelHash() parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractTruncateModel,
			Value: []byte("hash"),
		}
	}
}

// parTruncateModelReplace 指定字符串替代Value
// @param str
// @return parameterBuilder
func parTruncateModelReplace(str string) parameterBuilder {
	return func() *common.KeyValuePair {
		return &common.KeyValuePair{
			Key:   utils.KeyBlockContractTruncateModel,
			Value: []byte(str),
		}
	}
}
func (cc *ChainClient) queryBlockInfo(queryBlockFunc string, parameter ...parameterBuilder) (*common.BlockInfo, error) {
	var parameters []*common.KeyValuePair
	for _, p := range parameter {
		parameters = append(parameters, p())
	}
	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		queryBlockFunc, parameters, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		if utils.IsArchived(resp.Code) {
			return nil, errors.New(resp.Code.String())
		}
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	blockInfo := &common.BlockInfo{}
	if err = proto.Unmarshal(resp.ContractResult.Result, blockInfo); err != nil {
		return nil, fmt.Errorf("%s unmarshal block info payload failed, %s", queryBlockFunc, err)
	}

	return blockInfo, nil
}

// GetBlockByHeight get block by block height, returns *common.BlockInfo
func (cc *ChainClient) GetBlockByHeight(blockHeight uint64, withRWSet bool) (*common.BlockInfo, error) {
	if cc.IsArchiveCenterQueryFist() {
		// 首先去归档中心查询
		blk, err := cc.GetArchiveService().GetBlockByHeight(blockHeight, withRWSet)
		if err == nil && blk != nil {
			return blk, nil
		}
	}
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[blockHeight:%d]/[withRWSet:%t]",
		syscontract.ChainQueryFunction_GET_BLOCK_BY_HEIGHT, blockHeight, withRWSet)
	return cc.queryBlockInfo(syscontract.ChainQueryFunction_GET_BLOCK_BY_HEIGHT.String(),
		parBlockHeight(blockHeight), parWithRWSet(withRWSet))
}

// GetBlockByHeightTruncate 根据区块高度获得区块，应对超大block，可设置返回block的截断规则。
// @param blockHeight
// @param withRWSet
// @param truncateLength 截断的长度设置，如果此参数为>0，超过此长度的剩余数据将被丢弃。如果<=0则不截断
// @param truncateModel 截断的模式设置:hash,truncate,empty
// @return *common.BlockInfo
// @return error
func (cc *ChainClient) GetBlockByHeightTruncate(blockHeight uint64, withRWSet bool, truncateLength int,
	truncateModel string) (*common.BlockInfo, error) {
	if cc.IsArchiveCenterQueryFist() {
		// 首先去归档中心查询
		blk, err := cc.GetArchiveService().GetBlockByHeight(blockHeight, withRWSet)
		if err == nil && blk != nil {
			return blk, nil
		}
		//TODO 归档中心支持截断
	}

	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[blockHeight:%d]/[withRWSet:%t]",
		syscontract.ChainQueryFunction_GET_BLOCK_BY_HEIGHT, blockHeight, withRWSet)
	var model parameterBuilder
	switch strings.ToLower(truncateModel) {
	case "hash":
		model = parTruncateModelHash()
	case "truncate":
		model = parTruncateModelTruncate()
	case "empty":
		model = parTruncateModelEmpty()
	default:
		model = parTruncateModelReplace(truncateModel)
	}
	return cc.queryBlockInfo(syscontract.ChainQueryFunction_GET_BLOCK_BY_HEIGHT.String(),
		parBlockHeight(blockHeight), parWithRWSet(withRWSet),
		parTruncateValueLen(truncateLength), model)
}

// GetBlockByHash get block by block hash, returns *common.BlockInfo
func (cc *ChainClient) GetBlockByHash(blockHash string, withRWSet bool) (*common.BlockInfo, error) {
	if cc.IsArchiveCenterQueryFist() {
		// 首先去归档中心查询
		blk, err := cc.GetArchiveService().GetBlockByHash(blockHash, withRWSet)
		if err == nil && blk != nil {
			return blk, nil
		}
	}

	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[blockHash:%s]/[withRWSet:%t]",
		syscontract.ChainQueryFunction_GET_BLOCK_BY_HASH, blockHash, withRWSet)
	return cc.queryBlockInfo(syscontract.ChainQueryFunction_GET_BLOCK_BY_HASH.String(),
		parBlockHash(blockHash), parWithRWSet(withRWSet))
}

// GetBlockByTxId get block by tx id, returns *common.BlockInfo
func (cc *ChainClient) GetBlockByTxId(txId string, withRWSet bool) (*common.BlockInfo, error) {
	if cc.IsArchiveCenterQueryFist() {
		// 首先去归档中心查询
		blk, err := cc.GetArchiveService().GetBlockByTxId(txId, withRWSet)
		if err == nil && blk != nil {
			return blk, nil
		}
	}

	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[txId:%s]/[withRWSet:%t]",
		syscontract.ChainQueryFunction_GET_BLOCK_BY_TX_ID, txId, withRWSet)
	return cc.queryBlockInfo(syscontract.ChainQueryFunction_GET_BLOCK_BY_TX_ID.String(),
		parTxId(txId), parWithRWSet(withRWSet))
}

// GetLastConfigBlock get last config block
func (cc *ChainClient) GetLastConfigBlock(withRWSet bool) (*common.BlockInfo, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[withRWSet:%t]",
		syscontract.ChainQueryFunction_GET_LAST_CONFIG_BLOCK, withRWSet)
	return cc.queryBlockInfo(syscontract.ChainQueryFunction_GET_LAST_CONFIG_BLOCK.String(),
		parWithRWSet(withRWSet))
}

// GetChainInfo get chain info
func (cc *ChainClient) GetChainInfo() (*discovery.ChainInfo, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]", syscontract.ChainQueryFunction_GET_CHAIN_INFO)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_CHAIN_INFO.String(), []*common.KeyValuePair{}, defaultSeq, nil,
	)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	chainInfo := &discovery.ChainInfo{}
	if err = proto.Unmarshal(resp.ContractResult.Result, chainInfo); err != nil {
		return nil, fmt.Errorf("GetChainInfo unmarshal chain info payload failed, %s", err)
	}

	return chainInfo, nil
}

// GetNodeChainList get node chain list
func (cc *ChainClient) GetNodeChainList() (*discovery.ChainList, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]",
		syscontract.ChainQueryFunction_GET_NODE_CHAIN_LIST)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_NODE_CHAIN_LIST.String(), []*common.KeyValuePair{}, defaultSeq, nil,
	)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	chainList := &discovery.ChainList{}
	if err = proto.Unmarshal(resp.ContractResult.Result, chainList); err != nil {
		return nil, fmt.Errorf("GetNodeChainList unmarshal chain list payload failed, %s", err)
	}

	return chainList, nil
}

// GetFullBlockByHeight Deprecated, get full block by block height, returns *store.BlockWithRWSet
func (cc *ChainClient) GetFullBlockByHeight(blockHeight uint64) (*store.BlockWithRWSet, error) {
	if cc.IsArchiveCenterQueryFist() {
		// 首先去归档中心查询
		blk, err := cc.GetArchiveService().GetBlockByHeight(blockHeight, true)
		if err == nil && blk != nil {
			return &store.BlockWithRWSet{
				Block:          blk.Block,
				TxRWSets:       blk.RwsetList,
				ContractEvents: nil,
			}, nil
		}
	}

	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[blockHeight:%d]",
		syscontract.ChainQueryFunction_GET_FULL_BLOCK_BY_HEIGHT, blockHeight)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_FULL_BLOCK_BY_HEIGHT.String(), []*common.KeyValuePair{
			{
				Key:   utils.KeyBlockContractBlockHeight,
				Value: []byte(strconv.FormatUint(blockHeight, 10)),
			},
		}, defaultSeq, nil,
	)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		if utils.IsArchived(resp.Code) {
			return nil, errors.New(resp.Code.String())
		}
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	fullBlockInfo := &store.BlockWithRWSet{}
	if err = proto.Unmarshal(resp.ContractResult.Result, fullBlockInfo); err != nil {
		return nil, fmt.Errorf("GetFullBlockByHeight unmarshal block info payload failed, %s", err)
	}

	return fullBlockInfo, nil
}

// GetArchivedBlockHeight get archived block height
func (cc *ChainClient) GetArchivedBlockHeight() (uint64, error) {
	return cc.getBlockHeight("", "")
}

// GetBlockHeightByTxId get block height by tx id
func (cc *ChainClient) GetBlockHeightByTxId(txId string) (uint64, error) {
	return cc.getBlockHeight(txId, "")
}

// GetBlockHeightByHash get block height by block hash
func (cc *ChainClient) GetBlockHeightByHash(blockHash string) (uint64, error) {
	return cc.getBlockHeight("", blockHash)
}

func (cc *ChainClient) getBlockHeight(txId, blockHash string) (uint64, error) {
	//归档的时候不会删除TxId->Block 和BlockHash->Height的两个索引，所以直接从节点查询即可
	var (
		txType       = common.TxType_QUERY_CONTRACT
		contractName = syscontract.SystemContract_CHAIN_QUERY.String()
		method       string
		pairs        []*common.KeyValuePair
	)

	if txId != "" {
		method = syscontract.ChainQueryFunction_GET_BLOCK_HEIGHT_BY_TX_ID.String()
		pairs = []*common.KeyValuePair{
			{
				Key:   utils.KeyBlockContractTxId,
				Value: []byte(txId),
			},
		}

		cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[txId:%s]", method, txId)
	} else if blockHash != "" {
		method = syscontract.ChainQueryFunction_GET_BLOCK_HEIGHT_BY_HASH.String()
		pairs = []*common.KeyValuePair{
			{
				Key:   utils.KeyBlockContractBlockHash,
				Value: []byte(blockHash),
			},
		}

		cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[blockHash:%s]", method, blockHash)
	} else {
		method = syscontract.ChainQueryFunction_GET_ARCHIVED_BLOCK_HEIGHT.String()
		pairs = []*common.KeyValuePair{}

		cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]", method)
	}

	payload := cc.CreatePayload("", txType, contractName, method, pairs, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return 0, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return 0, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	blockHeight, err := strconv.ParseUint(string(resp.ContractResult.Result), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s, parse block height failed, %s", payload.TxType, err)
	}

	return blockHeight, nil
}

// GetLastBlock get last block
func (cc *ChainClient) GetLastBlock(withRWSet bool) (*common.BlockInfo, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[withRWSet:%t]",
		syscontract.ChainQueryFunction_GET_LAST_BLOCK, withRWSet)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_LAST_BLOCK.String(), []*common.KeyValuePair{
			{
				Key:   utils.KeyBlockContractWithRWSet,
				Value: []byte(strconv.FormatBool(withRWSet)),
			},
		}, defaultSeq, nil,
	)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	blockInfo := &common.BlockInfo{}
	if err = proto.Unmarshal(resp.ContractResult.Result, blockInfo); err != nil {
		return nil, fmt.Errorf("GetLastBlock unmarshal block info payload failed, %s", err)
	}

	return blockInfo, nil
}

// GetCurrentBlockHeight get current block height
func (cc *ChainClient) GetCurrentBlockHeight() (uint64, error) {
	block, err := cc.GetLastBlock(false)
	if err != nil {
		return 0, err
	}

	return block.Block.Header.BlockHeight, nil
}

// GetBlockHeaderByHeight get block header by block height
func (cc *ChainClient) GetBlockHeaderByHeight(blockHeight uint64) (*common.BlockHeader, error) {
	if cc.IsArchiveCenterQueryFist() {
		// 首先去归档中心查询
		blk, err := cc.GetArchiveService().GetBlockByHeight(blockHeight, false)
		if err == nil && blk != nil {
			return blk.Block.Header, nil
		}
	}

	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[blockHeight:%d]",
		syscontract.ChainQueryFunction_GET_BLOCK_HEADER_BY_HEIGHT, blockHeight)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_BLOCK_HEADER_BY_HEIGHT.String(), []*common.KeyValuePair{
			{
				Key:   utils.KeyBlockContractBlockHeight,
				Value: []byte(strconv.FormatUint(blockHeight, 10)),
			},
		}, defaultSeq, nil,
	)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	blockHeader := &common.BlockHeader{}
	if err = proto.Unmarshal(resp.ContractResult.Result, blockHeader); err != nil {
		return nil, fmt.Errorf("GetBlockHeaderByHeight unmarshal block header payload failed, %s", err)
	}

	return blockHeader, nil
}

// InvokeSystemContract invoke system contract
func (cc *ChainClient) InvokeSystemContract(contractName, method, txId string, kvs []*common.KeyValuePair,
	timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	cc.logger.Debugf("[SDK] begin to INVOKE system contract, [contractName:%s]/[method:%s]/[txId:%s]/[params:%+v]",
		contractName, method, txId, kvs)

	payload := cc.CreatePayload(txId, common.TxType_INVOKE_CONTRACT, contractName, method, kvs, defaultSeq, nil)

	return cc.proposalRequest(payload, nil, nil, timeout, withSyncResult)
}

// QuerySystemContract query system contract
func (cc *ChainClient) QuerySystemContract(contractName, method string, kvs []*common.KeyValuePair,
	timeout int64) (*common.TxResponse, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [contractName:%s]/[method:%s]/[params:%+v]",
		contractName, method, kvs)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, contractName, method, kvs, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, timeout, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	return resp, nil
}

// GetMerklePathByTxId get merkle path by tx id
func (cc *ChainClient) GetMerklePathByTxId(txId string) ([]byte, error) {
	//TODO
	//if cc.IsArchiveCenterQueryFist() {
	//	// 首先去归档中心查询
	//	httpResp, _ := cc.httpQueryArchiveCenter(archiveCenterApiGetMerklePathByTxId,
	//		&ArchiveCenterQueryParam{
	//			TxId: txId,
	//		}, reflect.TypeOf(ArchiveCenterResponseMerklePath{}))
	//	if httpResp != nil {
	//		merklePath, merklePathOk := httpResp.([]byte)
	//		// 归档中心查到有效数据
	//		if merklePathOk {
	//			return merklePath, nil
	//		}
	//	}
	//}
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]/[txId:%s]",
		syscontract.ChainQueryFunction_GET_MERKLE_PATH_BY_TX_ID, txId)

	kvs := []*common.KeyValuePair{
		{
			Key:   utils.KeyBlockContractTxId,
			Value: []byte(txId),
		},
	}

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_MERKLE_PATH_BY_TX_ID.String(), kvs, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}
	return resp.ContractResult.Result, nil
}

func (cc *ChainClient) createNativeContractAccessPayload(method string,
	accessContractList []string) (*common.Payload, error) {
	val, err := json.Marshal(accessContractList)
	if err != nil {
		return nil, err
	}
	kvs := []*common.KeyValuePair{
		{
			Key:   syscontract.ContractAccess_NATIVE_CONTRACT_NAME.String(),
			Value: val,
		},
	}
	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CONTRACT_MANAGE.String(),
		method, kvs, defaultSeq, nil), nil
}

// CreateNativeContractAccessGrantPayload create `native contract access grant` payload
// for grant access to a native contract
func (cc *ChainClient) CreateNativeContractAccessGrantPayload(grantContractList []string) (*common.Payload, error) {
	return cc.createNativeContractAccessPayload(syscontract.ContractManageFunction_GRANT_CONTRACT_ACCESS.String(),
		grantContractList)
}

// CreateNativeContractAccessRevokePayload create `native contract access revoke` payload
// for revoke access to a native contract
func (cc *ChainClient) CreateNativeContractAccessRevokePayload(revokeContractList []string) (*common.Payload, error) {
	return cc.createNativeContractAccessPayload(syscontract.ContractManageFunction_REVOKE_CONTRACT_ACCESS.String(),
		revokeContractList)
}

// GetContractInfo get contract info by contract name, returns *common.Contract
func (cc *ChainClient) GetContractInfo(contractName string) (*common.Contract, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]",
		syscontract.ContractQueryFunction_GET_CONTRACT_INFO)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CONTRACT_MANAGE.String(),
		syscontract.ContractQueryFunction_GET_CONTRACT_INFO.String(), []*common.KeyValuePair{
			{
				Key:   syscontract.GetContractInfo_CONTRACT_NAME.String(),
				Value: []byte(contractName),
			},
		}, defaultSeq, nil,
	)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	contract := &common.Contract{}
	if err = json.Unmarshal(resp.ContractResult.Result, contract); err != nil {
		return nil, fmt.Errorf("GetContractInfo unmarshal failed, %s", err)
	}

	return contract, nil
}

// GetContractList get all contracts from block chain, include user contract and system contract
func (cc *ChainClient) GetContractList() ([]*common.Contract, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]",
		syscontract.ContractQueryFunction_GET_CONTRACT_LIST)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CONTRACT_MANAGE.String(),
		syscontract.ContractQueryFunction_GET_CONTRACT_LIST.String(), nil, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	var contracts []*common.Contract
	if err = json.Unmarshal(resp.ContractResult.Result, &contracts); err != nil {
		return nil, fmt.Errorf("GetContractList unmarshal failed, %s", err)
	}

	return contracts, nil
}

// GetDisabledNativeContractList get all disabled native contracts, returns contracts name list
func (cc *ChainClient) GetDisabledNativeContractList() ([]string, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]",
		syscontract.ContractQueryFunction_GET_DISABLED_CONTRACT_LIST)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CONTRACT_MANAGE.String(),
		syscontract.ContractQueryFunction_GET_DISABLED_CONTRACT_LIST.String(), nil, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	var contractNames []string
	if err = json.Unmarshal(resp.ContractResult.Result, &contractNames); err != nil {
		return nil, fmt.Errorf("GetDisabledNativeContractList unmarshal failed, %s", err)
	}

	return contractNames, nil
}

// GetArchiveStatus get peer archive status
func (cc *ChainClient) GetArchiveStatus() (*store.ArchiveStatus, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]",
		syscontract.ChainQueryFunction_GET_ARCHIVE_STATUS)
	pairs := []*common.KeyValuePair{}
	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_CHAIN_QUERY.String(),
		syscontract.ChainQueryFunction_GET_ARCHIVE_STATUS.String(), pairs, defaultSeq, nil)
	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return nil, fmt.Errorf(errStringFormat, payload.TxType, err)
	}
	cc.logger.Debugf("[SDK] GetArchiveStatus original %v", *resp)
	var archiveStatus store.ArchiveStatus
	if err = proto.Unmarshal(resp.ContractResult.Result, &archiveStatus); err != nil {
		return nil, fmt.Errorf("GetArchiveStatus unmarshal failed , %v", err)
	}
	cc.logger.Debugf("[SDK] GetArchiveStatus %v", archiveStatus)
	return &archiveStatus, nil
}
