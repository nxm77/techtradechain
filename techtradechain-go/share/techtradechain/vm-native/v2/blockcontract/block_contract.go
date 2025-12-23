/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package blockcontract

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/localconf/v2"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	discoveryPb "techtradechain.com/techtradechain/pb-go/v2/discovery"
	storage "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"github.com/gogo/protobuf/proto"
)

const (
	paramNameBlockHeight      = "blockHeight"
	paramNameWithRWSet        = "withRWSet"
	paramNameBlockHash        = "blockHash"
	paramNameTxId             = "txId"
	paramNameTruncateValueLen = "truncateValueLen"
	paramNameTruncateModel    = "truncateModel"
	// TRUE string const
	TRUE = "true"
)

var (
	logTemplateMarshalBlockInfoFailed = "marshal block info failed, %s"
	errStoreIsNil                     = fmt.Errorf("store is nil")
	//errDataNotFound                   = fmt.Errorf("data not found")
)

type (
	getBlockHeightByTxId int64
	getHeightByHash      int64
)

// BlockContract 区块查询合约对象
type BlockContract struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewBlockContract BlockContract构造函数
// @param log
// @return *BlockContract
func NewBlockContract(log protocol.Logger) *BlockContract {
	return &BlockContract{
		log:     log,
		methods: registerBlockContractMethods(log),
	}
}

// GetMethod get register method by name
func (c *BlockContract) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

func registerBlockContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	q := make(map[string]common.ContractFunc, 64)
	b := &BlockRuntime{log: log}

	q[syscontract.ChainQueryFunction_GET_BLOCK_BY_HEIGHT.String()] = common.WrapResultFunc(b.GetBlockByHeight)
	q[syscontract.ChainQueryFunction_GET_BLOCK_WITH_TXRWSETS_BY_HEIGHT.String()] = common.WrapResultFunc(
		b.GetBlockWithTxRWSetsByHeight)
	q[syscontract.ChainQueryFunction_GET_BLOCK_BY_HASH.String()] = common.WrapResultFunc(b.GetBlockByHash)
	q[syscontract.ChainQueryFunction_GET_BLOCK_WITH_TXRWSETS_BY_HASH.String()] = common.WrapResultFunc(
		b.GetBlockWithTxRWSetsByHash)
	q[syscontract.ChainQueryFunction_GET_BLOCK_BY_TX_ID.String()] = common.WrapResultFunc(b.GetBlockByTxId)
	q[syscontract.ChainQueryFunction_GET_TX_BY_TX_ID.String()] = common.WrapResultFunc(b.GetTxByTxId)
	q[syscontract.ChainQueryFunction_GET_LAST_CONFIG_BLOCK.String()] = common.WrapResultFunc(b.GetLastConfigBlock)
	q[syscontract.ChainQueryFunction_GET_LAST_BLOCK.String()] = common.WrapResultFunc(b.GetLastBlock)
	q[syscontract.ChainQueryFunction_GET_CHAIN_INFO.String()] = common.WrapResultFunc(b.GetChainInfo)
	q[syscontract.ChainQueryFunction_GET_NODE_CHAIN_LIST.String()] = common.WrapResultFunc(b.GetNodeChainList)
	q[syscontract.ChainQueryFunction_GET_FULL_BLOCK_BY_HEIGHT.String()] = common.WrapResultFunc(b.GetFullBlockByHeight)
	q[syscontract.ChainQueryFunction_GET_BLOCK_HEIGHT_BY_TX_ID.String()] = common.WrapResultFunc(b.GetBlockHeightByTxId)
	q[syscontract.ChainQueryFunction_GET_BLOCK_HEIGHT_BY_HASH.String()] = common.WrapResultFunc(b.GetBlockHeightByHash)
	q[syscontract.ChainQueryFunction_GET_BLOCK_HEADER_BY_HEIGHT.String()] = common.WrapResultFunc(
		b.GetBlockHeaderByHeight)
	q[syscontract.ChainQueryFunction_GET_ARCHIVED_BLOCK_HEIGHT.String()] = common.WrapResultFunc(
		b.GetArchiveBlockHeight)
	q[syscontract.ChainQueryFunction_GET_ARCHIVE_STATUS.String()] = common.WrapResultFunc(
		b.GetArchiveStatus)
	q[syscontract.ChainQueryFunction_GET_MERKLE_PATH_BY_TX_ID.String()] = common.WrapResultFunc(b.GetMerklePathByTxId)
	return q
}

// BlockRuntime Block合约运行时
type BlockRuntime struct {
	log protocol.Logger
}

// BlockRuntimeParam 查询参数
type BlockRuntimeParam struct {
	height           uint64
	truncateValueLen int
	truncateModel    string //hash,truncate,empty
	withRWSet        bool
	hash             []byte
	txId             string
}

// validateParams 解析参数，并检查必须的参数选项
// @param parameters
// @param mustKeys 必须的参数列表
// @return *BlockRuntimeParam
// @return error
func (r *BlockRuntime) validateParams(parameters map[string][]byte, mustKeys ...string) (*BlockRuntimeParam, error) {
	mustPar := make(map[string]bool)
	for _, p := range mustKeys {
		mustPar[p] = false
	}
	var (
		errMsg string
		err    error
	)
	//解析传入的各个参数到对象BlockRuntimeParam中
	param := &BlockRuntimeParam{}
	for key, v := range parameters {
		switch key {
		case paramNameBlockHeight:
			value := string(v)
			if value == "-1" { //接收-1作为高度参数，用于表示最新高度，系统内部用MaxUint64表示最新高度
				param.height = math.MaxUint64
			} else {
				param.height, err = strconv.ParseUint(value, 10, 64)
			}
		case paramNameWithRWSet:
			if strings.ToLower(string(v)) == TRUE || string(v) == "1" {
				param.withRWSet = true
			}
		case paramNameBlockHash:
			param.hash, err = hex.DecodeString(string(v))
		case paramNameTxId:
			param.txId = string(v)
		case paramNameTruncateValueLen:
			value := string(v)
			param.truncateValueLen, err = strconv.Atoi(value)
		case paramNameTruncateModel:
			param.truncateModel = string(v)
		}
		if err != nil {
			return nil, err
		}
		if _, has := mustPar[key]; has {
			mustPar[key] = true
		}
	}
	//检查必须的参数是不是都提供了
	parsedMustKey := 0
	for _, show := range mustPar {
		if show {
			parsedMustKey++
		}
	}
	if parsedMustKey != len(mustKeys) {
		errMsg = fmt.Sprintf("invalid params len, need [%s]", strings.Join(mustKeys, "|"))
		r.log.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	return param, nil
}

// queryBlock 抽象的根据参数获得对应的区块和读写集的方法
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) queryBlock(txSimContext protocol.TxSimContext, parameters map[string][]byte,
	storeQuery func(store protocol.BlockchainStore, p *BlockRuntimeParam) (*commonPb.Block, error),
	mustKey ...string) (
	[]byte, error) {
	var errMsg string
	var err error

	// check params
	var param *BlockRuntimeParam
	if param, err = r.validateParams(parameters, mustKey...); err != nil {
		return nil, err
	}

	chainId := txSimContext.GetTx().Payload.ChainId

	store := txSimContext.GetBlockchainStore()
	if store == nil {
		return nil, errStoreIsNil
	}

	var block *commonPb.Block
	var txRWSets []*commonPb.TxRWSet

	//note: if block no exist in store, it will return (nil,nil)
	if block, err = storeQuery(store, param); err != nil || block == nil {
		return nil, r.handleErrorSpecTypeName(block, err, "block", chainId)
	}

	if param.withRWSet {
		if txRWSets, err = r.getTxRWSetsByBlock(store, chainId, block); err != nil {
			return nil, err
		}
	}
	if block, txRWSets, _, err = checkRoleAndFilterBlockTxs(block, txSimContext, txRWSets, nil); err != nil {
		return nil, err
	}
	blockInfo := &commonPb.BlockInfo{
		Block:     block,
		RwsetList: txRWSets,
	}
	//truncate
	if param.truncateValueLen > 0 {
		truncate := common.NewTruncateConfig(param.truncateValueLen, param.truncateModel)
		truncate.TruncateBlockWithRWSet(blockInfo)
	}
	blockInfoBytes, err := proto.Marshal(blockInfo)
	if err != nil {
		errMsg = fmt.Sprintf(logTemplateMarshalBlockInfoFailed, err.Error())
		r.log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	return blockInfoBytes, nil

}

// GetBlockByHeight 根据高度查询区块
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetBlockByHeight(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	return r.queryBlock(txSimContext, parameters,
		func(store protocol.BlockchainStore, p *BlockRuntimeParam) (*commonPb.Block, error) {
			if p.height == math.MaxUint64 {
				return store.GetLastBlock()
			}
			return store.GetBlock(p.height)
		}, paramNameBlockHeight)
}

// GetBlockWithTxRWSetsByHeight 根据高度查询区块和读写集
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetBlockWithTxRWSetsByHeight(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	parameters[paramNameWithRWSet] = []byte("true")
	return r.queryBlock(txSimContext, parameters,
		func(store protocol.BlockchainStore, p *BlockRuntimeParam) (*commonPb.Block, error) {
			if p.height == math.MaxUint64 {
				return store.GetLastBlock()
			}
			return store.GetBlock(p.height)
		}, paramNameBlockHeight)
}

// GetBlockByHash 根据区块Hash获得区块
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetBlockByHash(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	return r.queryBlock(txSimContext, parameters,
		func(store protocol.BlockchainStore, p *BlockRuntimeParam) (*commonPb.Block, error) {
			return store.GetBlockByHash(p.hash)
		}, paramNameBlockHash)
}

// GetBlockWithTxRWSetsByHash 根据区块Hash获得区块和读写集
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetBlockWithTxRWSetsByHash(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	parameters[paramNameWithRWSet] = []byte("true")
	return r.queryBlock(txSimContext, parameters,
		func(store protocol.BlockchainStore, p *BlockRuntimeParam) (*commonPb.Block, error) {
			return store.GetBlockByHash(p.hash)
		}, paramNameBlockHash)
}

// GetBlockByTxId 根据TxID获得所在的区块
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetBlockByTxId(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	return r.queryBlock(txSimContext, parameters,
		func(store protocol.BlockchainStore, p *BlockRuntimeParam) (*commonPb.Block, error) {
			return store.GetBlockByTx(p.txId)
		}, paramNameTxId)
}

// GetLastConfigBlock 获得最新的配置区块
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetLastConfigBlock(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	return r.queryBlock(txSimContext, parameters,
		func(store protocol.BlockchainStore, p *BlockRuntimeParam) (*commonPb.Block, error) {
			return store.GetLastConfigBlock()
		})
}

// GetLastBlock 获得最新的区块
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetLastBlock(txSimContext protocol.TxSimContext, parameters map[string][]byte) ([]byte, error) {
	return r.queryBlock(txSimContext, parameters,
		func(store protocol.BlockchainStore, p *BlockRuntimeParam) (*commonPb.Block, error) {
			return store.GetLastBlock()
		})
}

// GetNodeChainList 获得链ID列表 //TODO Devin:这和Block有啥关系？
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetNodeChainList(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	var errMsg string
	var err error
	var chainListBytes []byte

	// check params
	if _, err = r.validateParams(parameters); err != nil {
		return nil, err
	}

	blockChainConfigs := localconf.TechTradeChainConfig.GetBlockChains()
	chainIds := make([]string, len(blockChainConfigs))
	for i, blockChainConfig := range blockChainConfigs {
		chainIds[i] = blockChainConfig.ChainId
	}

	chainList := &discoveryPb.ChainList{
		ChainIdList: chainIds,
	}
	chainListBytes, err = proto.Marshal(chainList)
	if err != nil {
		errMsg = fmt.Sprintf("marshal chain list failed, %s", err.Error())
		r.log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	return chainListBytes, nil
}

// GetChainInfo 获得链信息
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetChainInfo(txSimContext protocol.TxSimContext, parameters map[string][]byte) ([]byte, error) {
	var errMsg string
	var err error

	// check params
	if _, err = r.validateParams(parameters); err != nil {
		return nil, err
	}

	chainId := txSimContext.GetTx().Payload.ChainId

	store := txSimContext.GetBlockchainStore()
	if store == nil {
		return nil, errStoreIsNil
	}

	var block *commonPb.Block
	var nodes []*discoveryPb.Node

	if block, err = r.getBlockByHeight(store, chainId, math.MaxUint64); err != nil {
		return nil, err
	}

	var provider protocol.ChainNodesInfoProvider
	if provider, err = txSimContext.GetChainNodesInfoProvider(); err != nil {
		r.log.Warn(err)
	} else {
		if nodes, err = r.getChainNodeInfo(provider, chainId); err != nil {
			return nil, err
		}
	}

	chainInfo := &discoveryPb.ChainInfo{
		BlockHeight: block.Header.BlockHeight,
		NodeList:    nodes,
	}

	chainInfoBytes, err := proto.Marshal(chainInfo)
	if err != nil {
		errMsg = fmt.Sprintf("marshal chain info failed, %s", err.Error())
		r.log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	return chainInfoBytes, nil
}

// GetMerklePathByTxId 获得一个交易的默克尔验证路径
// @param txSimContext
// @param parameters TxID
// @return []byte
// @return error
func (r *BlockRuntime) GetMerklePathByTxId(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	var errMsg string
	var err error
	var merkleTree [][]byte
	var merklePathsBytes []byte

	// check params
	var param *BlockRuntimeParam
	if param, err = r.validateParams(parameters, paramNameTxId); err != nil {
		return nil, err
	}

	chainId := txSimContext.GetTx().Payload.ChainId

	store := txSimContext.GetBlockchainStore()
	if store == nil {
		return nil, errStoreIsNil
	}

	chainConfig, err := store.GetLastChainConfig()
	if err != nil || chainConfig == nil {
		return nil, r.handleErrorSpecTypeName(chainConfig, err, "chainConfig", chainId)
	}
	hashType := chainConfig.Crypto.Hash

	var block *commonPb.Block

	if block, err = r.getBlockByTxId(store, chainId, param.txId); err != nil {
		return nil, err
	}

	hashes := make([][]byte, len(block.Txs))
	var index int32
	for i, tx := range block.Txs {
		var txHash []byte
		txHash, err = utils.CalcTxHashWithVersion(hashType, tx, int(block.Header.BlockVersion))
		if err != nil {
			return nil, err
		}
		hashes[i] = txHash
		if tx.Payload.TxId == param.txId {
			index = int32(i)
		}
	}

	merkleTree, err = hash.BuildMerkleTree(hashType, hashes)
	if err != nil {
		return nil, err
	}

	merklePaths := hash.GetMerklePath(index, merkleTree)

	merklePathsBytes, err = json.Marshal(merklePaths)
	if err != nil {
		errMsg = fmt.Sprintf(logTemplateMarshalBlockInfoFailed, err.Error())
		r.log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return merklePathsBytes, nil

}

// GetTxByTxId 根据TxId查询Transaction
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *BlockRuntime) GetTxByTxId(txSimContext protocol.TxSimContext, parameters map[string][]byte) ([]byte, error) {
	//var errMsg string
	var err error
	//var transactionInfoBytes []byte

	// check params
	var param *BlockRuntimeParam
	if param, err = r.validateParams(parameters, paramNameTxId); err != nil {
		return nil, err
	}
	//query
	chainId := txSimContext.GetTx().Payload.ChainId

	store := txSimContext.GetBlockchainStore()
	if store == nil {
		return nil, errStoreIsNil
	}
	var txInfoWithRWSet *commonPb.TransactionInfoWithRWSet
	if param.withRWSet {
		txInfoWithRWSet, err = r.getTxInfoWithRWSetByTxId(store, chainId, param.txId)
		if err != nil {
			return nil, err
		}
	} else {
		txInfo, err1 := r.getTxWithInfoByTxId(store, chainId, param.txId)
		if err1 != nil {
			return nil, err1
		}
		txInfoWithRWSet = &commonPb.TransactionInfoWithRWSet{
			Transaction:    txInfo.Transaction,
			BlockHeight:    txInfo.BlockHeight,
			BlockHash:      txInfo.BlockHash,
			TxIndex:        txInfo.TxIndex,
			BlockTimestamp: txInfo.BlockTimestamp,
			RwSet:          nil,
		}
	}
	//truncate
	if param.truncateValueLen > 0 {
		truncate := common.NewTruncateConfig(param.truncateValueLen, param.truncateModel)
		truncate.TruncateTx(txInfoWithRWSet.Transaction)
	}
	//access control

	if txInfoWithRWSet, err = checkRoleAndGenerateTransactionInfo(txSimContext, txInfoWithRWSet); err != nil {
		return nil, err
	}
	//Marshal返回结果
	transactionInfoBytes, err := proto.Marshal(txInfoWithRWSet)
	if err != nil {
		errMsg := fmt.Sprintf("marshal tx failed, %s", err.Error())
		r.log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	return transactionInfoBytes, nil

}

// GetFullBlockByHeight 获得完整的区块信息（包括了合约事件，而且不得裁剪）
// @param txSimContext
// @param params
// @return []byte
// @return error
func (r *BlockRuntime) GetFullBlockByHeight(txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte,
	error) {
	var errMsg string
	var err error
	var block *commonPb.Block
	var blockWithRWSetBytes []byte

	// check params
	var param *BlockRuntimeParam
	if param, err = r.validateParams(params, paramNameBlockHeight); err != nil {
		return nil, err
	}

	chainId := txSimContext.GetTx().Payload.ChainId

	store := txSimContext.GetBlockchainStore()
	if store == nil {
		return nil, errStoreIsNil
	}

	var blockWithRWSet *storage.BlockWithRWSet
	if blockWithRWSet, err = r.getFullBlockByHeight(store, chainId, param.height); err != nil {
		return nil, err
	}

	if block, blockWithRWSet.TxRWSets, blockWithRWSet.ContractEvents, err = checkRoleAndFilterBlockTxs(
		blockWithRWSet.Block, txSimContext, blockWithRWSet.TxRWSets, blockWithRWSet.ContractEvents); err != nil {
		return nil, err
	}

	blockWithRWSet = &storage.BlockWithRWSet{
		Block:          block,
		TxRWSets:       blockWithRWSet.TxRWSets,
		ContractEvents: blockWithRWSet.ContractEvents,
	}

	blockWithRWSetBytes, err = blockWithRWSet.Marshal()
	if err != nil {
		errMsg = fmt.Sprintf("marshal block with rwset failed, %s", err.Error())
		r.log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return blockWithRWSetBytes, nil
}

// GetBlockHeightByTxId 根据TxID获得所在的区块高度
// @param txSimContext
// @param params
// @return []byte
// @return error
func (r *BlockRuntime) GetBlockHeightByTxId(txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte,
	error) {
	var err error

	// check params
	var param *BlockRuntimeParam
	if param, err = r.validateParams(params, paramNameTxId); err != nil {
		return nil, err
	}

	chainId := txSimContext.GetTx().Payload.ChainId

	store := txSimContext.GetBlockchainStore()
	if store == nil {
		return nil, errStoreIsNil
	}

	var blockHeight uint64
	blockHeight, err = r.getBlockHeightByTxId(store, chainId, param.txId)
	if err != nil {
		return nil, err
	}

	resultBlockHeight := strconv.FormatInt(int64(blockHeight), 10)
	return []byte(resultBlockHeight), nil
}

// GetBlockHeightByHash 根据区块Hash获得该区块的高度
// @param txSimContext
// @param params
// @return []byte
// @return error
func (r *BlockRuntime) GetBlockHeightByHash(txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte,
	error) {
	var err error
	var blockHeight uint64

	// check params
	var param *BlockRuntimeParam
	if param, err = r.validateParams(params, paramNameBlockHash); err != nil {
		return nil, err
	}

	chainId := txSimContext.GetTx().Payload.ChainId
	store := txSimContext.GetBlockchainStore()
	if store == nil {
		return nil, errStoreIsNil
	}

	blockHeight, err = r.getBlockHeightByHash(store, chainId, param.hash)
	if err != nil {
		return nil, err
	}

	resultBlockHeight := strconv.FormatInt(int64(blockHeight), 10)
	return []byte(resultBlockHeight), nil
}

// GetBlockHeaderByHeight 根据区块高度获得区块头
// @param txSimContext
// @param params
// @return []byte
// @return error
func (r *BlockRuntime) GetBlockHeaderByHeight(txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte,
	error) {
	var err error
	var errMsg string
	var blockHeaderBytes []byte

	// check params
	var param *BlockRuntimeParam
	if param, err = r.validateParams(params, paramNameBlockHeight); err != nil {
		return nil, err
	}

	chainId := txSimContext.GetTx().Payload.ChainId

	store := txSimContext.GetBlockchainStore()
	if store == nil {
		return nil, errStoreIsNil
	}

	var blockHeader *commonPb.BlockHeader
	if blockHeader, err = r.getBlockHeaderByHeight(store, chainId, param.height); err != nil {
		return nil, err
	}

	blockHeaderBytes, err = blockHeader.Marshal()
	if err != nil {
		errMsg = fmt.Sprintf("block header marshal err is %s ", err.Error())
		r.log.Error(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return blockHeaderBytes, nil
}

func (r *BlockRuntime) getChainNodeInfo(provider protocol.ChainNodesInfoProvider, chainId string) (
	[]*discoveryPb.Node, error) {
	nodeInfos, err := provider.GetChainNodesInfo()
	if err != nil {
		r.log.Errorf("get chain node info failed, [chainId:%s], %s", chainId, err.Error())
		return nil, fmt.Errorf("get chain node info failed failed, %s", err)
	}
	nodes := make([]*discoveryPb.Node, len(nodeInfos))
	for i, nodeInfo := range nodeInfos {
		nodes[i] = &discoveryPb.Node{
			NodeId:      nodeInfo.NodeUid,
			NodeAddress: strings.Join(nodeInfo.NodeAddress, ","),
			NodeTlsCert: nodeInfo.NodeTlsCert,
		}
	}
	return nodes, nil
}

func (r *BlockRuntime) getBlockByHeight(store protocol.BlockchainStore, chainId string, height uint64) (
	*commonPb.Block, error) {
	var (
		block *commonPb.Block
		err   error
	)

	if height == math.MaxUint64 {
		block, err = store.GetLastBlock()
	} else {
		block, err = store.GetBlock(height)
	}
	err = r.handleError(block, err, chainId)
	return block, err
}

func (r *BlockRuntime) getFullBlockByHeight(store protocol.BlockchainStore, chainId string, height uint64) (
	*storage.BlockWithRWSet, error) {
	var (
		lastBlock      *commonPb.Block
		blockWithRWSet *storage.BlockWithRWSet
		err            error
	)

	if height == math.MaxUint64 {
		lastBlock, err = store.GetLastBlock()
		if err != nil {
			err = r.handleError(lastBlock, err, chainId)
			return nil, err
		}

		blockWithRWSet, err = store.GetBlockWithRWSets(lastBlock.Header.BlockHeight)
	} else {
		blockWithRWSet, err = store.GetBlockWithRWSets(height)
	}

	err = r.handleError(blockWithRWSet, err, chainId)
	return blockWithRWSet, err
}

func (r *BlockRuntime) getBlockHeaderByHeight(store protocol.BlockchainStore, chainId string, height uint64) (
	*commonPb.BlockHeader, error) {
	var (
		lastBlock   *commonPb.Block
		blockHeader *commonPb.BlockHeader
		err         error
	)

	if height == math.MaxUint64 {
		lastBlock, err = store.GetLastBlock()
		if err != nil {
			err = r.handleError(lastBlock, err, chainId)
			return nil, err
		}

		blockHeader, err = store.GetBlockHeaderByHeight(lastBlock.Header.BlockHeight)
	} else {
		blockHeader, err = store.GetBlockHeaderByHeight(height)
	}

	err = r.handleError(blockHeader, err, chainId)
	return blockHeader, err
}
func (r *BlockRuntime) getBlockHeightByTxId(store protocol.BlockchainStore, chainId string, txId string) (
	uint64, error) {
	height, err := store.GetTxHeight(txId)
	err = r.handleError(getBlockHeightByTxId(height), err, chainId)
	return height, err
}

func (r *BlockRuntime) getBlockHeightByHash(store protocol.BlockchainStore, chainId string, hash []byte) (
	uint64, error) {

	height, err := store.GetHeightByHash(hash)
	err = r.handleError(getHeightByHash(height), err, chainId)
	return height, err
}

func (r *BlockRuntime) getBlockByTxId(store protocol.BlockchainStore, chainId string, txId string) (
	*commonPb.Block, error) {
	block, err := store.GetBlockByTx(txId)
	err = r.handleError(block, err, chainId)
	return block, err
}

func (r *BlockRuntime) getTxWithInfoByTxId(store protocol.BlockchainStore, chainId string, txId string) (
	*commonPb.TransactionInfo, error) {
	tx, err := store.GetTxWithInfo(txId)
	err = r.handleErrorSpecTypeName(tx, err, "transaction", chainId)
	return tx, err
}
func (r *BlockRuntime) getTxInfoWithRWSetByTxId(store protocol.BlockchainStore, chainId string, txId string) (
	*commonPb.TransactionInfoWithRWSet, error) {
	tx, err := store.GetTxInfoWithRWSet(txId)
	err = r.handleErrorSpecTypeName(tx, err, "transaction", chainId)
	return tx, err
}
func (r *BlockRuntime) getTxRWSetsByBlock(store protocol.BlockchainStore, chainId string, block *commonPb.Block) (
	[]*commonPb.TxRWSet, error) {
	var txRWSets []*commonPb.TxRWSet
	for _, tx := range block.Txs {
		txRWSet, err := store.GetTxRWSet(tx.Payload.TxId)
		if err != nil {
			r.log.Errorf("get txRWset from store failed, [chainId:%s|txId:%s], %s",
				chainId, tx.Payload.TxId, err.Error())
			return nil, fmt.Errorf("get txRWset failed, %s", err)
		}
		if txRWSet == nil { //数据库未找到记录，这不正常，记录日志，初始化空实例
			r.log.Errorf("not found rwset data in database by txid=%d, please check database", tx.Payload.TxId)
			txRWSet = &commonPb.TxRWSet{}
		}
		txRWSets = append(txRWSets, txRWSet)
	}
	return txRWSets, nil
}

// GetArchiveBlockHeight 获得已经归档的区块高度
// @param context
// @param params
// @return []byte
// @return error
func (r *BlockRuntime) GetArchiveBlockHeight(context protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	blockHeight := strconv.FormatInt(int64(context.GetBlockchainStore().GetArchivedPivot()), 10)

	r.log.Infof("get archive block height success blockHeight[%s] ", blockHeight)
	return []byte(blockHeight), nil
}

// GetArchiveStatus 获得归档信息
// @param context
// @param params
// @return []byte
// @return error
func (r *BlockRuntime) GetArchiveStatus(context protocol.TxSimContext, params map[string][]byte) ([]byte, error) {
	as, err := context.GetBlockchainStore().GetArchiveStatus()
	if err != nil {
		errMsg := fmt.Sprintf("get archive status failed: %s", err.Error())
		r.log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	bytes, errb := proto.Marshal(as)
	if errb != nil {
		errMsg := fmt.Sprintf(logTemplateMarshalBlockInfoFailed, errb.Error())
		r.log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	r.log.Infof("get archive status success archive pivot[%d] ", as.ArchivePivot)
	return bytes, nil
}

func (r *BlockRuntime) handleError(value interface{}, err error, chainId string) error {
	tn := strings.Split(fmt.Sprintf("%T", value), ".")
	typeName := strings.ToLower(tn[len(tn)-1])
	return r.handleErrorSpecTypeName(value, err, typeName, chainId)
}

func (r *BlockRuntime) handleErrorSpecTypeName(value interface{}, err error, typeName, chainId string) error {
	if err != nil {
		r.log.Errorf("get %s from store failed, [chainId:%s], %s", typeName, chainId, err.Error())
		return fmt.Errorf("get %s failed, %s", typeName, err)
	}
	vi := reflect.ValueOf(value)
	if vi.Kind() == reflect.Ptr && vi.IsNil() {
		errMsg := fmt.Errorf("no such %s, chainId:%s", typeName, chainId)
		r.log.Warnf(errMsg.Error())
		return errMsg
	}
	return nil
}
