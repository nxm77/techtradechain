/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"techtradechain.com/techtradechain/common/v2/json"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
)

var _ ArchiveService = (*ArchiveCenterHttpClient)(nil)

// ArchiveCenterHttpClient 归档服务的归档中心HTTP客户端实现，只读
type ArchiveCenterHttpClient struct {
	logger utils.Logger
	config *ArchiveCenterConfig
}

// Register 只读查询，不支持注册
// @param genesis
// @return error
func (a *ArchiveCenterHttpClient) Register(genesis *common.BlockInfo) error {
	panic("http is a read only api, not implement Register")
}

// ArchiveBlock 只读查询，不支持归档区块
// @param block
// @return error
func (a *ArchiveCenterHttpClient) ArchiveBlock(block *common.BlockInfo) error {
	panic("http is a read only api, not implement ArchiveBlock")
}

// ArchiveBlocks 只读查询，不支持归档区块
// @param bi
// @return error
func (a *ArchiveCenterHttpClient) ArchiveBlocks(bi BlockIterator, _ func(ProcessMessage) error) error {
	panic("http is a read only api, not implement ArchiveBlocks")
}

// GetChainConfigByBlockHeight 根据高度获得链配置
// @param blockHeight
// @return *config.ChainConfig
// @return error
func (a *ArchiveCenterHttpClient) GetChainConfigByBlockHeight(blockHeight uint64) (*config.ChainConfig, error) {
	httpResp, err := a.httpQueryArchiveCenter(archiveCenterApiGetConfigByHeight,
		&ArchiveCenterQueryParam{
			Height: blockHeight,
		}, reflect.TypeOf(ArchiveCenterResponseChainConfig{}))
	if err != nil {
		return nil, err
	}
	if httpResp != nil {
		config, configOk := httpResp.(*config.ChainConfig)
		// 归档中心查到有效数据
		if configOk {
			return config, nil
		}
	}
	return nil, nil
}

// ArchiveCenterResponse 定义了归档中心通用返回头信息
type ArchiveCenterResponse struct {
	Code     int    `json:"code"` // 错误码,0代表成功.其余代表失败
	ErrorMsg string `json:"errorMsg"`
	//Data     interface{} `json:"data"`
}

// ArchiveCenterResponseTransaction 定义交易返回结构
type ArchiveCenterResponseTransaction struct {
	ArchiveCenterResponse
	Data *common.TransactionInfo `json:"data"`
}

// ArchiveCenterResponseTransactionWithRwSet 定义读写集交易
type ArchiveCenterResponseTransactionWithRwSet struct {
	ArchiveCenterResponse
	Data *common.TransactionInfoWithRWSet `json:"data"`
}

// ArchiveCenterResponseBlockInfo 定义区块返回结构
type ArchiveCenterResponseBlockInfo struct {
	ArchiveCenterResponse
	Data *common.BlockInfo `json:"data"`
}

// ArchiveCenterResponseBlockWithRwSet 定义带读写集区块
type ArchiveCenterResponseBlockWithRwSet struct {
	ArchiveCenterResponse
	Data *common.BlockInfo `json:"data"`
}

// ArchiveCenterResponseHeight 定义块高返回结构
type ArchiveCenterResponseHeight struct {
	ArchiveCenterResponse
	Data *uint64 `json:"data"`
}

// ArchiveCenterResponseBlockHeader 定义区块头返回结构
type ArchiveCenterResponseBlockHeader struct {
	ArchiveCenterResponse
	Data *common.BlockHeader `json:"data"`
}

// ArchiveCenterResponseChainConfig 定义链配置返回结构
type ArchiveCenterResponseChainConfig struct {
	ArchiveCenterResponse
	Data *config.ChainConfig `json:"data"`
}

// ArchiveCenterResponseMerklePath 定义merkle path 返回结构
type ArchiveCenterResponseMerklePath struct {
	ArchiveCenterResponse
	Data []byte `json:"data"`
}

// ArchiveCenterConfig 定义归档中心配置
type ArchiveCenterConfig struct {
	ChainGenesisHash     string //
	ArchiveCenterHttpUrl string
	ReqeustSecondLimit   int // http请求的超时间隔,默认5秒
	RpcAddress           string
	TlsEnable            bool
	Tls                  utils.TlsConfig
	MaxSendMsgSize       int
	MaxRecvMsgSize       int
}

// ArchiveCenterQueryParam 定义归档中心通用请求头
type ArchiveCenterQueryParam struct {
	ChainGenesisHash string `json:"chain_genesis_hash,omitempty"`
	Start            uint64 `json:"start,omitempty"`
	End              uint64 `json:"end,omitempty"`
	BlockHash        string `json:"block_hash,omitempty"`
	Height           uint64 `json:"height,omitempty"`
	TxId             string `json:"tx_id,omitempty"`
	WithRwSet        bool   `json:"with_rwset,omitempty"`
	TruncateLength   int    `json:"truncate_length,omitempty"`
	TruncateModel    string `json:"truncate_model,omitempty"`
}

var (
	// httpRequestDuration 默认http请求超时秒数
	httpRequestDuration        = 5
	archiveCenterRpcMaxMsgSize = 200
)

var (
	// archiveCenterApiGetBlockHeaderByHeight 根据高度查区块头接口
	//archiveCenterApiGetBlockHeaderByHeight = "get_block_header_by_height"
	// archiveCenterApiGetBlockWithTxRWSetByHash 根据hash查读写集区块接口
	archiveCenterApiGetBlockWithTxRWSetByHash = "get_block_info_by_hash"
	// archiveCenterApiGetBlockWithTxRWSetByTxId 根据txid查读写集区块接口
	archiveCenterApiGetBlockWithTxRWSetByTxId = "get_block_info_by_txid"
	// archiveCenterApiGetBlockWithTxRWSetByHeight 根据高度查读写集区块接口
	archiveCenterApiGetBlockWithTxRWSetByHeight = "get_block_info_by_height"
	// archiveCenterApiGetCommonTransactionByTxId 根据txid查交易接口
	archiveCenterApiGetCommonTransactionByTxId = "get_transaction_info_by_txid"
	// archiveCenterApiGetCommonTransactionWithRWSetByTxId 根据txid查读写集交易接口
	archiveCenterApiGetCommonTransactionWithRWSetByTxId = "get_full_transaction_info_by_txid"
	// archiveCenterApiGetConfigByHeight 根据块高查询最近链配置信息接口
	archiveCenterApiGetConfigByHeight = "get_chainconfig_by_height"
	// archiveCenterApiGetMerklePathByTxId 根据txid查询merkle path
	//archiveCenterApiGetMerklePathByTxId = "get_merklepath_by_txid"
	// archiveCenterApiGetTruncateTxByTxId 根据txid查询截断交易信息
	//archiveCenterApiGetTruncateTxByTxId = "get_truncate_tx_by_txid"
	// archiveCenterApiGetTruncateBlockByHeight 根据高度查询截断区块信息
	//archiveCenterApiGetTruncateBlockByHeight = "get_truncate_block_by_height"
)

func (cc *ArchiveCenterHttpClient) httpQueryArchiveCenter(apiUri string,
	queryParam *ArchiveCenterQueryParam,
	responseT reflect.Type) (interface{}, error) {
	cfg := cc.config
	if cfg == nil || len(cfg.ArchiveCenterHttpUrl) == 0 {
		return nil, nil
	}
	queryParam.ChainGenesisHash = cfg.ChainGenesisHash // 这里设置一下genesisBlockHash
	// 构造请求body
	reqBody, _ := json.Marshal(queryParam)
	requestUri := getApiUrl(apiUri, cfg.ArchiveCenterHttpUrl)
	ctx, ctxCancel := context.WithTimeout(context.Background(),
		time.Duration(cfg.ReqeustSecondLimit)*time.Second)
	defer ctxCancel()
	// 构造http请求
	req, reqErr := http.NewRequestWithContext(ctx,
		http.MethodPost, requestUri, bytes.NewReader(reqBody))
	if reqErr != nil {
		cc.logger.Errorf("httpQueryArchiveCenter api[%s] NewRequest got error [%s]",
			apiUri, reqErr.Error())
		return nil, reqErr
	}
	// 请求http接口
	resp, respErr := http.DefaultClient.Do(req)
	if respErr != nil {
		cc.logger.Errorf("httpQueryArchiveCenter api[%s] do request error [%s]",
			apiUri, respErr.Error())
		return nil, respErr
	}
	if resp == nil {
		cc.logger.Warnf("httpQueryArchiveCenter api[%s] got nothing", apiUri)
		return nil, nil
	}
	defer resp.Body.Close()
	// 读取接口返回信息
	respBytes, respBytesErr := ioutil.ReadAll(resp.Body)
	if respBytesErr != nil {
		cc.logger.Errorf("httpQueryArchiveCenter api[%s] read resp body error [%s]",
			apiUri, respBytesErr.Error())
		return nil, respBytesErr
	}
	responseV := reflect.New(responseT)
	responseP := responseV.Interface()
	unMarshalErr := json.Unmarshal(respBytes, responseP)
	if unMarshalErr != nil {
		cc.logger.Errorf("httpQueryArchiveCenter api[%s] unMarshal error [%s], origin resp [%s]",
			apiUri, unMarshalErr.Error(), string(respBytes))
		return nil, unMarshalErr
	}
	// 解析接口返回数据
	code := responseV.Elem().FieldByName("Code").Int()
	// 读取错误信息
	msg := responseV.Elem().FieldByName("ErrorMsg").String()
	// 读取业务数据
	data := responseV.Elem().FieldByName("Data").Interface()
	// 判断是否有有效数据
	dataIsNil := responseV.Elem().FieldByName("Data").IsNil()
	// 查询错误
	if code > 0 {
		cc.logger.Warnf("httpQueryArchiveCenter api[%s] query got code[%d] error[%s] ",
			apiUri, code, msg)
		return nil, fmt.Errorf("code[%d] msg[%s]", code, msg)
	}
	// 无有效数据
	if dataIsNil {
		cc.logger.Warnf("httpQueryArchiveCenter api[%s] query got nothing", apiUri)
		return nil, nil
	}
	return data, nil
}

func getApiUrl(apiUrl, baseUrl string) string {
	return fmt.Sprintf("%s/%s", baseUrl, apiUrl)
}

// NewArchiveCenterHttpClient ArchiveCenterHttpClient构造函数
// @param config
// @param log
// @return *ArchiveCenterHttpClient
func NewArchiveCenterHttpClient(config *ArchiveCenterConfig, log utils.Logger) *ArchiveCenterHttpClient {
	return &ArchiveCenterHttpClient{
		logger: log,
		config: config,
	}
}

// ArchiveCenterConfig 获得归档中心配置
// @return *ArchiveCenterConfig
func (a *ArchiveCenterHttpClient) ArchiveCenterConfig() *ArchiveCenterConfig {
	return a.config
}

// Close 关闭
// @return error
func (_ *ArchiveCenterHttpClient) Close() error {
	return nil
}

// GetTxByTxId 根据TxId获得交易
// @param txId
// @return *common.TransactionInfo
// @return error
func (a *ArchiveCenterHttpClient) GetTxByTxId(txId string) (*common.TransactionInfo, error) {
	httpResp, httpRespErr := a.httpQueryArchiveCenter(archiveCenterApiGetCommonTransactionByTxId,
		&ArchiveCenterQueryParam{
			TxId: txId,
		}, reflect.TypeOf(ArchiveCenterResponseTransaction{}))
	if httpResp != nil {
		tx, transactionOk := httpResp.(*common.TransactionInfo)
		// 归档中心查到有效数据
		if transactionOk {
			return tx, nil
		}
	}
	if httpRespErr != nil {
		return nil, httpRespErr
	}
	return nil, nil
}

// GetTxWithRWSetByTxId 根据TxId获得交易和读写集
// @param txId
// @return *common.TransactionInfoWithRWSet
// @return error
func (a *ArchiveCenterHttpClient) GetTxWithRWSetByTxId(txId string) (*common.TransactionInfoWithRWSet, error) {
	httpResp, httpRespErr := a.httpQueryArchiveCenter(archiveCenterApiGetCommonTransactionWithRWSetByTxId,
		&ArchiveCenterQueryParam{
			TxId: txId,
		}, reflect.TypeOf(ArchiveCenterResponseTransaction{}))
	if httpResp != nil {
		tx, transactionOk := httpResp.(*common.TransactionInfoWithRWSet)
		// 归档中心查到有效数据
		if transactionOk {
			return tx, nil
		}
	}
	if httpRespErr != nil {
		return nil, httpRespErr
	}
	return nil, nil
}

// GetBlockByHeight 根据高度获得区块
// @param blockHeight
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveCenterHttpClient) GetBlockByHeight(blockHeight uint64, withRWSet bool) (*common.BlockInfo, error) {
	httpResp, httpRespErr := a.httpQueryArchiveCenter(archiveCenterApiGetBlockWithTxRWSetByHeight,
		&ArchiveCenterQueryParam{
			Height: blockHeight,
		}, reflect.TypeOf(ArchiveCenterResponseBlockWithRwSet{}))
	if httpResp != nil {
		block, blockOK := httpResp.(*common.BlockInfo)
		// 归档中心查到有效数据
		if blockOK {
			return block, nil
		}
	}
	if httpRespErr != nil {
		return nil, httpRespErr
	}
	return nil, nil
}

// GetBlockByHash 根据Hash获得区块
// @param blockHash
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveCenterHttpClient) GetBlockByHash(blockHash string, withRWSet bool) (*common.BlockInfo, error) {
	httpResp, httpRespErr := a.httpQueryArchiveCenter(archiveCenterApiGetBlockWithTxRWSetByHash,
		&ArchiveCenterQueryParam{
			BlockHash: blockHash,
		}, reflect.TypeOf(ArchiveCenterResponseBlockInfo{}))
	if httpResp != nil {
		block, blockOk := httpResp.(*common.BlockInfo)
		// 归档中心查到有效数据
		if blockOk {
			var retBlock common.BlockInfo
			retBlock.Block = block.Block
			if withRWSet {
				retBlock.RwsetList = block.RwsetList
			}
			return &retBlock, nil
		}
	}
	if httpRespErr != nil {
		return nil, httpRespErr
	}
	return nil, nil
}

// GetBlockByTxId 根据TxId获得区块
// @param txId
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveCenterHttpClient) GetBlockByTxId(txId string, withRWSet bool) (*common.BlockInfo, error) {
	httpResp, httpRespErr := a.httpQueryArchiveCenter(archiveCenterApiGetBlockWithTxRWSetByTxId,
		&ArchiveCenterQueryParam{
			TxId: txId,
		}, reflect.TypeOf(ArchiveCenterResponseBlockInfo{}))
	if httpRespErr != nil {
		return nil, httpRespErr
	}

	if httpResp != nil {
		block, blockOk := httpResp.(*common.BlockInfo)
		// 归档中心查到有效数据
		if blockOk {
			var retBlock common.BlockInfo
			retBlock.Block = block.Block
			if withRWSet {
				retBlock.RwsetList = block.RwsetList
			}
			return &retBlock, nil
		}
	}

	return nil, nil
}

// GetArchivedStatus 获得归档中心状态
// @return archivedHeight
// @return inArchive
// @return code
// @return err
func (a *ArchiveCenterHttpClient) GetArchivedStatus() (archivedHeight uint64, inArchive bool, code uint32, err error) {
	panic("not implement GetArchivedStatus")
	//TODO
}
