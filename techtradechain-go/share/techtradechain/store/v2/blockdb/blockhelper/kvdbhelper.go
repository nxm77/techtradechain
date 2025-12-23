/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0

*/

package blockhelper

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"

	bytesconv "techtradechain.com/techtradechain/common/v2/bytehelper"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/store/v2/cache"
	"techtradechain.com/techtradechain/store/v2/serialization"
	"techtradechain.com/techtradechain/store/v2/types"
	tbf "techtradechain.com/techtradechain/store/v2/types/blockfile"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// nolint
const (
	BlockNumIdxKeyPrefix  = 'n'
	BlockHashIdxKeyPrefix = 'h'
	TxIDIdxKeyPrefix      = 't'
	//TxConfirmedTimeKeyPrefix = 'c'
	BlockTxIDIdxKeyPrefix = 'b'
	LastBlockNumKeyStr    = "lastBlockNumKey"
	LastConfigBlockNumKey = "lastConfigBlockNumKey"
	ArchivedPivotKey      = "archivedPivotKey"
)

// DbType_Mysql nolint
var DbType_Mysql = "mysql"

// BuildKVBatch add next time
// @Description:
// @param batch
// @param blockInfo
// @param dbType
// @param logger
// @return *types.UpdateBatch
func BuildKVBatch(isBFDB bool, batch *types.UpdateBatch, blockInfo *serialization.BlockWithSerializedInfo,
	dbType string, logger protocol.Logger) {
	if batch == nil {
		return
	}

	block := blockInfo.Block
	if isBFDB {
		batch = BuildKVBatchForBFDB(batch, block, dbType, logger)
	} else {
		batch = BuildKVBatchForRawDB(batch, block, dbType, logger)
	}

	// 1. height-> blockInfo
	heightKey := ConstructBlockNumKey(dbType, block.Header.BlockHeight)
	batch.Put(heightKey, blockInfo.SerializedMeta)

	// 2. txid -> tx,  txid -> blockHeight
	txConfirmedTime := make([]byte, 8)
	binary.BigEndian.PutUint64(txConfirmedTime, uint64(block.Header.BlockTimestamp))
	logger.Debugf("[blockdb]1 CommitCache currtime[%d]", utils.CurrentTimeMillisSeconds())
	// 3. Concurrency batch Put
	//并发更新cache,ConcurrencyMap,提升并发更新能力
	wg := &sync.WaitGroup{}
	wg.Add(len(blockInfo.SerializedTxs))
	for index, txBytes := range blockInfo.SerializedTxs {
		go func(index int, txBytes []byte, batch protocol.StoreBatcher, wg *sync.WaitGroup) {
			defer wg.Done()
			tx := blockInfo.Block.Txs[index]

			// if block file db disable, save tx data to db
			txIdKey := ConstructTxIDKey(tx.Payload.TxId)
			batch.Put(txIdKey, txBytes)

			// 把tx的地址写入数据库
			blockTxIdKey := ConstructBlockTxIDKey(tx.Payload.TxId)
			txBlockInf := ConstructTxIDBlockInfo(block.Header.BlockHeight, block.Header.BlockHash, uint32(index),
				block.Header.BlockTimestamp)

			batch.Put(blockTxIdKey, txBlockInf)
			//b.logger.Debugf("put tx[%s]", tx.Payload.TxId)
		}(index, txBytes, batch, wg)
	}

	logger.Debugf("[blockdb]2 CommitCache currtime[%d]", utils.CurrentTimeMillisSeconds())
	wg.Wait()
	logger.Debugf("[blockdb]3 CommitCache currtime[%d]", utils.CurrentTimeMillisSeconds())
}

//// BuildKVBatch add next time
//// @Description:
//// @param batch
//// @param blockInfo
//// @param dbType
//// @param logger
//// @return *types.UpdateBatch
//func BuildKVBatch(isBFDB bool, batch *types.UpdateBatch, blockInfo *serialization.BlockWithSerializedInfo,
//	dbType string, logger protocol.Logger) {
//	if batch == nil {
//		return
//	}
//
//	block := blockInfo.Block
//	if isBFDB {
//
//	} else {
//		batch = BuildKVBatchForRawDB(batch, blockInfo, dbType, logger)
//	}
//
//	if isBFDB {
//		batch.Put([]byte(LastBlockNumKeyStr), EncodeBlockNum(block.Header.BlockHeight))
//	} else {
//		lastBlockNumBytes := make([]byte, 8)
//		binary.BigEndian.PutUint64(lastBlockNumBytes, block.Header.BlockHeight)
//		batch.Put([]byte(LastBlockNumKeyStr), lastBlockNumBytes)
//	}
//
//	// 2. height-> blockInfo
//	heightKey := ConstructBlockNumKey(dbType, block.Header.BlockHeight)
//	batch.Put(heightKey, blockInfo.SerializedMeta)
//	logger.Infof("---houfa--- BuildKVBatch block[%d], key: %v, value len: %d",
//		block.Header.BlockHeight, heightKey, len(blockInfo.SerializedMeta))
//
//	// 4. hash-> height
//	hashKey := ConstructBlockHashKey(dbType, block.Header.BlockHash)
//	if isBFDB {
//		batch.Put(hashKey, EncodeBlockNum(block.Header.BlockHeight))
//	} else {
//		batch.Put(hashKey, heightKey)
//	}
//
//	// 4. txid -> tx,  txid -> blockHeight
//	txConfirmedTime := make([]byte, 8)
//	binary.BigEndian.PutUint64(txConfirmedTime, uint64(block.Header.BlockTimestamp))
//	logger.Debugf("[blockdb]1 CommitCache currtime[%d]", utils.CurrentTimeMillisSeconds())
//	// 5. Concurrency batch Put
//	//并发更新cache,ConcurrencyMap,提升并发更新能力
//	wg := &sync.WaitGroup{}
//	wg.Add(len(blockInfo.SerializedTxs))
//	for index, txBytes := range blockInfo.SerializedTxs {
//		go func(index int, txBytes []byte, batch protocol.StoreBatcher, wg *sync.WaitGroup) {
//			defer wg.Done()
//			tx := blockInfo.Block.Txs[index]
//
//			// if block file db disable, save tx data to db
//			txIdKey := ConstructTxIDKey(tx.Payload.TxId)
//			batch.Put(txIdKey, txBytes)
//
//			// 把tx的地址写入数据库
//			blockTxIdKey := ConstructBlockTxIDKey(tx.Payload.TxId)
//			txBlockInf := ConstructTxIDBlockInfo(block.Header.BlockHeight, block.Header.BlockHash, uint32(index),
//				block.Header.BlockTimestamp)
//
//			batch.Put(blockTxIdKey, txBlockInf)
//			//b.logger.Debugf("put tx[%s]", tx.Payload.TxId)
//		}(index, txBytes, batch, wg)
//	}
//
//	logger.Debugf("[blockdb]2 CommitCache currtime[%d]", utils.CurrentTimeMillisSeconds())
//	wg.Wait()
//	logger.Debugf("[blockdb]3 CommitCache currtime[%d]", utils.CurrentTimeMillisSeconds())
//
//	// last configBlock height
//	if block.Header.BlockHeight == 0 || utils.IsConfBlock(blockInfo.Block) {
//		if isBFDB {
//			batch.Put([]byte(LastConfigBlockNumKey), EncodeBlockNum(block.Header.BlockHeight))
//		} else {
//			batch.Put([]byte(LastConfigBlockNumKey), heightKey)
//		}
//		logger.Infof("chain[%s]: commit config blockInfo[%d]", block.Header.ChainId, block.Header.BlockHeight)
//	}
//}

// BuildKVBatchForRawDB add next time
// @Description:
// @param batch
// @param blockInfo
// @param dbType
// @param logger
// @return *types.UpdateBatch
func BuildKVBatchForRawDB(batch *types.UpdateBatch, block *commonPb.Block,
	dbType string, logger protocol.Logger) *types.UpdateBatch {
	if batch == nil {
		return batch
	}

	// 1. lastBlockNum -> height
	lastBlockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lastBlockNumBytes, block.Header.BlockHeight)
	batch.Put([]byte(LastBlockNumKeyStr), lastBlockNumBytes)

	// 2. hash-> height
	heightKey := ConstructBlockNumKey(dbType, block.Header.BlockHeight)
	hashKey := ConstructBlockHashKey(dbType, block.Header.BlockHash)
	batch.Put(hashKey, heightKey)

	// 3. last configBlock height
	if block.Header.BlockHeight == 0 || utils.IsConfBlock(block) {
		batch.Put([]byte(LastConfigBlockNumKey), heightKey)
		logger.Infof("chain[%s]: commit config blockInfo[%d]", block.Header.ChainId, block.Header.BlockHeight)
	}

	return batch
}

// BuildKVBatchForBFDB add next time
// @Description:
// @param batch
// @param blockInfo
// @param dbType
// @param logger
// @return *types.UpdateBatch
func BuildKVBatchForBFDB(batch *types.UpdateBatch, block *commonPb.Block,
	dbType string, logger protocol.Logger) *types.UpdateBatch {
	if batch == nil {
		return batch
	}

	// 1. last blockNum -> height
	batch.Put([]byte(LastBlockNumKeyStr), EncodeBlockNum(block.Header.BlockHeight))

	// 2. hash-> height
	hashKey := ConstructBlockHashKey(dbType, block.Header.BlockHash)
	batch.Put(hashKey, EncodeBlockNum(block.Header.BlockHeight))

	// 3. last configBlock height
	if block.Header.BlockHeight == 0 || utils.IsConfBlock(block) {
		batch.Put([]byte(LastConfigBlockNumKey), EncodeBlockNum(block.Header.BlockHeight))
		logger.Infof("chain[%s]: commit config blockInfo[%d]", block.Header.ChainId, block.Header.BlockHeight)
	}

	return batch
}

// ConstructBlockNumKey construct blockNum key
// format : n{blockNum}
//
//	@Description:
//	@param dbType
//	@param blockNum
//	@return []byte
func ConstructBlockNumKey(dbType string, blockNum uint64) []byte {
	var blkNumBytes []byte
	if dbType == DbType_Mysql {
		blkNumBytes = []byte(fmt.Sprint(blockNum))
	} else {
		blkNumBytes = EncodeBlockNum(blockNum)
	}
	return append([]byte{BlockNumIdxKeyPrefix}, blkNumBytes...)
}

// ConstructBlockHashKey construct block hash key
// format: h{blockHash}
//
//	@Description:
//	@param dbType
//	@param blockHash
//	@return []byte
func ConstructBlockHashKey(dbType string, blockHash []byte) []byte {
	if dbType == DbType_Mysql {
		bString := hex.EncodeToString(blockHash)
		return append([]byte{BlockHashIdxKeyPrefix}, bString...)
	}
	return append([]byte{BlockHashIdxKeyPrefix}, blockHash...)
}

// ConstructTxIDKey construct tx id key
// format : t{txId}
//
//	@Description:
//	@param txId
//	@return []byte
func ConstructTxIDKey(txId string) []byte {
	return append([]byte{TxIDIdxKeyPrefix}, bytesconv.StringToBytes(txId)...)
}

//func constructTxConfirmedTimeKey(txId string) []byte {
//	return append([]byte{txConfirmedTimeKeyPrefix}, txId...)
//}

// ConstructBlockTxIDKey construct block tx id key
// format : b{txID}
//
//	@Description:
//	@param txID
//	@return []byte
func ConstructBlockTxIDKey(txID string) []byte {
	//return append([]byte{BlockTxIDIdxKeyPrefix}, []byte(txID)...)
	return append([]byte{BlockTxIDIdxKeyPrefix}, bytesconv.StringToBytes(txID)...)
}

// EncodeBlockNum varint encode blockNum
//
//	@Description:
//	@param blockNum
//	@return []byte
func EncodeBlockNum(blockNum uint64) []byte {
	return proto.EncodeVarint(blockNum)
}

// DecodeBlockNumKey decode
//
//	@Description:
//	@param dbType
//	@param blkNumBytes
//	@return uint64
func DecodeBlockNumKey(dbType string, blkNumBytes []byte) uint64 {
	blkNumBytes = blkNumBytes[len([]byte{BlockNumIdxKeyPrefix}):]
	if dbType == DbType_Mysql {
		intNum, _ := strconv.Atoi(string(blkNumBytes))
		return uint64(intNum)
	}
	return DecodeBlockNum(blkNumBytes)
}

// DecodeBlockNum varint decode
//
//	@Description:
//	@param blockNumBytes
//	@return uint64
func DecodeBlockNum(blockNumBytes []byte) uint64 {
	blockNum, _ := proto.DecodeVarint(blockNumBytes)
	return blockNum
}

// ConstructTxIDBlockInfo marshal TransactionStoreInfo
//
//	@Description:
//	@param height
//	@param blockHash
//	@param txIndex
//	@param timestamp
//	@return []byte
func ConstructTxIDBlockInfo(height uint64, blockHash []byte, txIndex uint32, timestamp int64) []byte {
	//value := fmt.Sprintf("%d,%x,%d", height, blockHash, txIndex)
	//return []byte(value)
	txInf := &storePb.TransactionStoreInfo{
		BlockHeight:    height,
		BlockHash:      blockHash,
		TxIndex:        txIndex,
		BlockTimestamp: timestamp,
	}
	data, _ := txInf.Marshal()
	return data
}

// ParseTxIdBlockInfoOld return height,blockHash,txIndex
//
//	@Description:
//	@param value
//	@return height
//	@return blockHash
//	@return txIndex
//	@return err
func ParseTxIdBlockInfoOld(value []byte) (height uint64, blockHash []byte, txIndex uint32, err error) {
	strArray := strings.Split(string(value), ",")
	if len(strArray) != 3 {
		err = fmt.Errorf("invalid input[%s]", value)
		return
	}
	height, err = strconv.ParseUint(strArray[0], 10, 64)
	if err != nil {
		return 0, nil, 0, err
	}
	blockHash, err = hex.DecodeString(strArray[1])
	if err != nil {
		return 0, nil, 0, err
	}
	idx, err := strconv.Atoi(strArray[2])
	txIndex = uint32(idx)
	return
}

// ParseTxIdBlockInfo return height,blockHash,txIndex，timestamp
//
//	@Description:
//	@param value
//	@return height
//	@return blockHash
//	@return txIndex
//	@return timestamp
//	@return err
func ParseTxIdBlockInfo(value []byte) (height uint64, blockHash []byte, txIndex uint32, timestamp int64, err error) {
	if len(value) == 0 {
		err = fmt.Errorf("input is empty,in parseTxIdBlockInfo")
		return
	}
	//新版本使用TransactionInfo，是因为经过BenchmarkTest速度会快很多
	var txInfo storePb.TransactionStoreInfo
	err = txInfo.Unmarshal(value)
	if err != nil {
		//兼容老版本存储
		height, blockHash, txIndex, err = ParseTxIdBlockInfoOld(value)
		return
	}
	height = txInfo.BlockHeight
	blockHash = txInfo.BlockHash
	txIndex = txInfo.TxIndex
	timestamp = txInfo.BlockTimestamp
	err = nil
	return
}

// GetBlockByHeightKey 根据区块高度Key获得区块信息和其中的交易信息
//
//	@Description:
//	@receiver b
//	@param heightKey
//	@param includeTxs
//	@return *commonPb.Block
//	@return error
func GetBlockByHeightKey(dbHandle protocol.DBHandle, cacheSC *cache.StoreCacheMgr,
	heightKey []byte, includeTxs bool, logger protocol.Logger,
	handleTx func(txId string, vBytes []byte, err error) (*commonPb.Transaction, error)) (*commonPb.Block, error) {
	if heightKey == nil {
		return nil, nil
	}

	vBytes, err := GetFromCacheFirst(heightKey, dbHandle, cacheSC, logger)
	if err != nil || vBytes == nil {
		return nil, err
	}

	var blockStoreInfo storePb.SerializedBlock
	err = proto.Unmarshal(vBytes, &blockStoreInfo)
	if err != nil {
		return nil, err
	}

	var block = commonPb.Block{
		Header:         blockStoreInfo.Header,
		Dag:            blockStoreInfo.Dag,
		AdditionalData: blockStoreInfo.AdditionalData,
	}
	if includeTxs {
		//var batchWG sync.WaitGroup
		//batchWG.Add(len(blockStoreInfo.TxIds))
		//errsChan := make(chan error, len(blockStoreInfo.TxIds))
		block.Txs = make([]*commonPb.Transaction, len(blockStoreInfo.TxIds))
		for index, txid := range blockStoreInfo.TxIds {
			//used to limit the num of concurrency goroutine
			//b.workersSemaphore.Acquire(context.Background(), 1)
			//go func(i int, txid string) {
			//	defer b.workersSemaphore.Release(1)
			//	defer batchWG.Done()
			//tx, err1 := b.GetTx(txid)
			v, errV := GetFromCacheFirst(ConstructTxIDKey(txid), dbHandle, cacheSC, logger)
			tx, err1 := handleTx(txid, v, errV)
			if err1 != nil {
				if err1 == tbf.ErrArchivedTx {
					return nil, tbf.ErrArchivedBlock
				}
				//errsChan <- err
				return nil, err1
			}

			block.Txs[index] = tx
			//}(index, txid)
		}
		//batchWG.Wait()
		//if len(errsChan) > 0 {
		//	return nil, <-errsChan
		//}
	}
	logger.Debugf("chain[%s]: get block[%d] with transactions[%d]",
		block.Header.ChainId, block.Header.BlockHeight, len(block.Txs))
	return &block, nil
}

// GetFromCacheFirst first from cache;if not found,from db
//
//	@Description:
//	@receiver b
//	@param key
//	@return []byte
//	@return error
func GetFromCacheFirst(key []byte, dbHandle protocol.DBHandle, cacheSC *cache.StoreCacheMgr,
	logger protocol.Logger) ([]byte, error) {
	//get from cache
	value, exist := cacheSC.Get(string(key))
	if exist {
		logger.Debugf("get content: [%x] by [%v] in cache", value, key)
		if len(value) == 0 {
			logger.Debugf("get value []byte is empty in cache, key[%v], value: %s, ", key, value)
		}
		return value, nil
	}
	//如果从db 中未找到，会返回 (val,err) 为 (nil,nil)
	//由调用get的上层函数再做判断
	//因为存储系统，对一个key/value 做删除操作，是 直接把 value改为 nil,不做物理删除
	//get from database
	val, err := dbHandle.Get(key)
	logger.Debugf("get content: [%x] by [%v] in database", val, key)
	if err != nil {
		if len(val) == 0 {
			logger.Debugf("get value []byte is empty in database, key[%v], value: %s, ", key, value)
		}
	}
	if err == nil {
		if len(val) == 0 {
			logger.Debugf("get value []byte is empty in database,err == nil, key[%v], value: %s, ", key, value)
		}
	}
	return val, err
}
