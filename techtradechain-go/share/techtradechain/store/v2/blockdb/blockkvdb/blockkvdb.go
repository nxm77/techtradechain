// Package blockkvdb package
package blockkvdb

/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0

*/
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	leveldbprovider "techtradechain.com/techtradechain/store-leveldb/v2"
	"techtradechain.com/techtradechain/store/v2/blockdb/blockhelper"
	"techtradechain.com/techtradechain/store/v2/cache"
	"techtradechain.com/techtradechain/store/v2/conf"
	"techtradechain.com/techtradechain/store/v2/serialization"
	"techtradechain.com/techtradechain/store/v2/types"
	tbf "techtradechain.com/techtradechain/store/v2/types/blockfile"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
	"golang.org/x/sync/semaphore"
)

var (
	errValueNotFound = errors.New("value not found")
	errGetBatchPool  = errors.New("get updatebatch error")
)

// BlockKvDB provider an implementation of `blockdb.BlockDB`
//
//	@Description:
//
// This implementation provides a key-value based data model
type BlockKvDB struct {
	sync.Mutex
	dbHandle         protocol.DBHandle
	workersSemaphore *semaphore.Weighted
	worker           int64
	cache            *cache.StoreCacheMgr
	archivedPivot    uint64
	logger           protocol.Logger
	batchPool        sync.Pool
	storeConfig      *conf.StorageConfig
}

// IsArchivedPivotKeyZeroStatus  return the archive status
// @Description: if the archive never occur , return true, else return false
//
//	this interface is used to distinguish the chain contains the block 0 or only lacks block 0
//	@return bool
func (b *BlockKvDB) IsArchivedPivotKeyZeroStatus() bool {
	heightBytes, err := b.dbHandle.Get([]byte(blockhelper.ArchivedPivotKey))
	if err != nil {
		return true
	}
	if heightBytes == nil {
		return true
	}
	return false
}

// NewBlockKvDB 创建blockKvDB
//
//	@Description:
//	@param chainId
//	@param dbHandle
//	@param logger
//	@param storeConfig
//	@return *BlockKvDB
func NewBlockKvDB(chainId string, dbHandle protocol.DBHandle, logger protocol.Logger,
	storeConfig *conf.StorageConfig) *BlockKvDB {
	nWorkers := int64(runtime.NumCPU())
	b := &BlockKvDB{
		dbHandle:         dbHandle,
		worker:           nWorkers,
		workersSemaphore: semaphore.NewWeighted(nWorkers),
		cache:            cache.NewStoreCacheMgr(chainId, 10, logger),
		archivedPivot:    0,
		logger:           logger,
		batchPool:        sync.Pool{},
		storeConfig:      storeConfig,
	}
	b.batchPool.New = func() interface{} {
		return types.NewUpdateBatch()
	}

	return b
}

// InitGenesis 初始化创世区块
//
//	@Description:
//	@receiver b
//	@param genesisBlock
//	@return error
func (b *BlockKvDB) InitGenesis(genesisBlock *serialization.BlockWithSerializedInfo) error {
	//update Cache
	err := b.CommitBlock(genesisBlock, true)
	if err != nil {
		return err
	}
	//update BlockKvDB
	return b.CommitBlock(genesisBlock, false)
}

// CommitBlock commits the block and the corresponding rwsets in an atomic operation
//
//	@Description:
//	@receiver b
//	@param blockInfo
//	@param isCache
//	@return error
func (b *BlockKvDB) CommitBlock(blockInfo *serialization.BlockWithSerializedInfo, isCache bool) error {
	//原则上，写db失败，重试一定次数后，仍然失败，panic

	//如果是更新cache，则 直接更新 然后返回，不写 blockdb
	if isCache {
		return b.CommitCache(blockInfo)
	}
	//如果不是更新cache，则 直接更新 kvdb，然后返回
	return b.CommitDB(blockInfo)
}

// CommitCache 提交数据到cache
//
//	@Description:
//	@receiver b
//	@param blockInfo
//	@return error
func (b *BlockKvDB) CommitCache(blockInfo *serialization.BlockWithSerializedInfo) error {
	b.logger.Debugf("[blockdb]start CommitCache currtime[%d]", utils.CurrentTimeMillisSeconds())
	//如果是更新cache，则 直接更新 然后返回，不写 blockdb
	//从对象池中取一个对象,并重置
	start := time.Now()
	batch, ok := b.batchPool.Get().(*types.UpdateBatch)
	if !ok {
		b.logger.Errorf("chain[%s]: blockInfo[%d] get updatebatch error",
			blockInfo.Block.Header.ChainId, blockInfo.Block.Header.BlockHeight)
		return errGetBatchPool
	}
	batch.ReSet()

	blockhelper.BuildKVBatch(false, batch, blockInfo, b.dbHandle.GetDbType(), b.logger)

	block := blockInfo.Block
	// 6. 增加cache,注意这个batch放到cache中了，正在使用，不能放到batchPool中
	b.cache.AddBlock(block.Header.BlockHeight, batch)

	b.logger.Debugf("chain[%s]: commit cache block[%d] blockdb, batch[%d], time used: %d",
		block.Header.ChainId, block.Header.BlockHeight, batch.Len(),
		time.Since(start).Milliseconds())
	return nil
}

// CommitDB 提交数据到kvdb
//
//	@Description:
//	@receiver b
//	@param blockInfo
//	@return error
func (b *BlockKvDB) CommitDB(blockInfo *serialization.BlockWithSerializedInfo) error {
	//1. 从缓存中取 batch
	start := time.Now()
	block := blockInfo.Block
	cacheBatch, err := b.cache.GetBatch(block.Header.BlockHeight)
	if err != nil {
		b.logger.Errorf("chain[%s]: commit blockInfo[%d] delete cacheBatch error",
			block.Header.ChainId, block.Header.BlockHeight)
		panic(err)
	}

	batchDur := time.Since(start)
	//2. write blockDb
	writeBatch := types.NewUpdateBatch()
	for k, v := range cacheBatch.KVs() {
		writeBatch.Put([]byte(k), v)
	}
	err = b.writeBatch(block.Header.BlockHeight, writeBatch)
	if err != nil {
		return err
	}

	//3. Delete block from Cache, put batch to batchPool
	//把cacheBatch 从Cache 中删除
	b.cache.DelBlock(block.Header.BlockHeight)

	//再把cacheBatch放回到batchPool 中 (注意这两步前后不能反了)
	b.batchPool.Put(cacheBatch)
	writeDur := time.Since(start)

	b.logger.Debugf("chain[%s]: commit block[%d] kv blockdb, time used (batch[%d]:%d, "+
		"write:%d, total:%d)", block.Header.ChainId, block.Header.BlockHeight, cacheBatch.Len(),
		batchDur.Milliseconds(), (writeDur - batchDur).Milliseconds(), time.Since(start).Milliseconds())
	return nil
}

// GetArchivedPivot return archived pivot
//
//	@Description:
//	@receiver b
//	@return uint64
//	@return error
func (b *BlockKvDB) GetArchivedPivot() (uint64, error) {
	heightBytes, err := b.dbHandle.Get([]byte(blockhelper.ArchivedPivotKey))
	if err != nil {
		return 0, err
	}

	// heightBytes can be nil while db do not have archive pivot, we use pivot 1 as default
	dbHeight := uint64(0)
	if heightBytes != nil {
		dbHeight = blockhelper.DecodeBlockNumKey(b.dbHandle.GetDbType(), heightBytes)
	}

	if dbHeight != b.archivedPivot {
		b.archivedPivot = dbHeight
	}

	return b.archivedPivot, nil
}

// ShrinkBlocks remove ranged txid--SerializedTx from kvdb
//
//	@Description:
//	@receiver b
//	@param startHeight
//	@param endHeight
//	@return map[uint64][]string
//	@return error
func (b *BlockKvDB) ShrinkBlocks(startHeight uint64, endHeight uint64, _ string) (map[uint64][]string, error) {
	var (
		block *commonPb.Block
		err   error
	)

	hKey := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), endHeight)
	if block, err = blockhelper.GetBlockByHeightKey(b.dbHandle, b.cache, hKey, true,
		b.logger, b.handleTx); err != nil {
		return nil, err
	}

	if utils.IsConfBlock(block) {
		return nil, tbf.ErrConfigBlockArchive
	}

	txIdsMap := make(map[uint64][]string)
	startTime := utils.CurrentTimeMillisSeconds()
	for height := startHeight; height <= endHeight; height++ {
		heightKey := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), height)
		blk, err1 := blockhelper.GetBlockByHeightKey(b.dbHandle, b.cache, heightKey, true, b.logger, b.handleTx)
		if err1 != nil {
			return nil, err1
		}

		if utils.IsConfBlock(blk) {
			b.logger.Infof("skip shrink conf block: [%d]", block.Header.BlockHeight)
			continue
		}

		batch := types.NewUpdateBatch()
		txIds := make([]string, 0, len(blk.Txs))
		for _, tx := range blk.Txs {
			// delete tx data
			batch.Delete(blockhelper.ConstructTxIDKey(tx.Payload.TxId))
			txIds = append(txIds, tx.Payload.TxId)
		}
		txIdsMap[height] = txIds
		//set archivedPivotKey to db
		batch.Put([]byte(blockhelper.ArchivedPivotKey), blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), height))

		//先删除cache，再更新db,删除key/value,就是把 value 重置成 nil
		b.cache.DelBlock(height)

		if err = b.dbHandle.WriteBatch(batch, true); err != nil {
			return nil, err
		}

		b.archivedPivot = height
	}

	go func() {
		_ = b.CompactRange()
	}()

	usedTime := utils.CurrentTimeMillisSeconds() - startTime
	b.logger.Infof("shrink block from [%d] to [%d] time used: %d",
		startHeight, endHeight, usedTime)
	return txIdsMap, nil
}

// RestoreBlocks restore block data from outside to kvdb: txid--SerializedTx
//
//	@Description:
//	@receiver b
//	@param blockInfos
//	@return error
func (b *BlockKvDB) RestoreBlocks(blockInfos []*serialization.BlockWithSerializedInfo) error {
	startTime := utils.CurrentTimeMillisSeconds()
	archivePivot := uint64(0)
	for i := len(blockInfos) - 1; i >= 0; i-- {
		blockInfo := blockInfos[i]

		//check whether block can be archived
		if utils.IsConfBlock(blockInfo.Block) {
			b.logger.Infof("skip store conf block: [%d]", blockInfo.Block.Header.BlockHeight)
			continue
		}

		//check block hash
		sBlock, err := b.GetFilteredBlock(blockInfo.Block.Header.BlockHeight)
		if err != nil {
			return err
		}

		if !bytes.Equal(blockInfo.Block.Header.BlockHash, sBlock.Header.BlockHash) {
			return tbf.ErrInvalidateRestoreBlocks
		}

		batch := types.NewUpdateBatch()
		//verify imported block txs
		for index, stx := range blockInfo.SerializedTxs {
			// put tx data
			batch.Put(blockhelper.ConstructTxIDKey(blockInfo.Block.Txs[index].Payload.TxId), stx)
		}

		archivePivot, err = b.getNextArchivePivot(blockInfo.Block)
		if err != nil {
			return err
		}

		batch.Put([]byte(blockhelper.ArchivedPivotKey),
			blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), archivePivot))
		err = b.dbHandle.WriteBatch(batch, true)
		if err != nil {
			return err
		}
		b.archivedPivot = archivePivot
	}

	go func() {
		_ = b.CompactRange()
	}()

	usedTime := utils.CurrentTimeMillisSeconds() - startTime
	b.logger.Infof("restore block from [%d] to [%d] time used: %d",
		blockInfos[len(blockInfos)-1].Block.Header.BlockHeight, blockInfos[0].Block.Header.BlockHeight, usedTime)
	return nil
}

// BlockExists returns true if the block hash exist, or returns false if none exists.
//
//	@Description:
//	@receiver b
//	@param blockHash
//	@return bool
//	@return error
func (b *BlockKvDB) BlockExists(blockHash []byte) (bool, error) {
	hashKey := blockhelper.ConstructBlockHashKey(b.dbHandle.GetDbType(), blockHash)
	return b.has(hashKey)
}

// GetBlockByHash returns a block given its hash, or returns nil if none exists.
//
//	@Description:
//	@receiver b
//	@param blockHash
//	@return *commonPb.Block
//	@return error
func (b *BlockKvDB) GetBlockByHash(blockHash []byte) (*commonPb.Block, error) {
	hashKey := blockhelper.ConstructBlockHashKey(b.dbHandle.GetDbType(), blockHash)
	heightBytes, err := blockhelper.GetFromCacheFirst(hashKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}

	return blockhelper.GetBlockByHeightKey(b.dbHandle, b.cache, heightBytes, true, b.logger, b.handleTx)
}

// GetHeightByHash returns a block height given its hash, or returns nil if none exists.
//
//	@Description:
//	@receiver b
//	@param blockHash
//	@return uint64
//	@return error
func (b *BlockKvDB) GetHeightByHash(blockHash []byte) (uint64, error) {
	hashKey := blockhelper.ConstructBlockHashKey(b.dbHandle.GetDbType(), blockHash)
	heightBytes, err := blockhelper.GetFromCacheFirst(hashKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return 0, err
	}

	if heightBytes == nil {
		return 0, errValueNotFound
	}

	return blockhelper.DecodeBlockNumKey(b.dbHandle.GetDbType(), heightBytes), nil
}

// GetBlockHeaderByHeight returns a block header by given its height, or returns nil if none exists.
//
//	rawdb archive do not remove block metadata
//	@Description:
//	@receiver b
//	@param height
//	@return *commonPb.BlockHeader
//	@return error
func (b *BlockKvDB) GetBlockHeaderByHeight(height uint64) (*commonPb.BlockHeader, error) {
	heightKey := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), height)
	vBytes, err := blockhelper.GetFromCacheFirst(heightKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}

	if vBytes == nil {
		return nil, errValueNotFound
	}

	var blockStoreInfo storePb.SerializedBlock
	err = proto.Unmarshal(vBytes, &blockStoreInfo)
	if err != nil {
		return nil, err
	}

	return blockStoreInfo.Header, nil
}

// GetBlock returns a block given its block height, or returns nil if none exists.
//
//	@Description:
//	@receiver b
//	@param height
//	@return *commonPb.Block
//	@return error
func (b *BlockKvDB) GetBlock(height uint64) (*commonPb.Block, error) {
	heightBytes := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), height)
	return blockhelper.GetBlockByHeightKey(b.dbHandle, b.cache, heightBytes, true, b.logger, b.handleTx)
}

// GetLastBlock returns the last block.
//
//	@Description:
//	@receiver b
//	@return *commonPb.Block
//	@return error
func (b *BlockKvDB) GetLastBlock() (*commonPb.Block, error) {
	num, err := b.GetLastSavepoint()
	if err != nil {
		return nil, err
	}

	heightBytes := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), num)
	return blockhelper.GetBlockByHeightKey(b.dbHandle, b.cache, heightBytes, true, b.logger, b.handleTx)
}

// GetLastConfigBlock returns the last config block.
//
//	@Description:
//	@receiver b
//	@return *commonPb.Block
//	@return error
func (b *BlockKvDB) GetLastConfigBlock() (*commonPb.Block, error) {
	heightKey, err := blockhelper.GetFromCacheFirst([]byte(blockhelper.LastConfigBlockNumKey),
		b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}
	b.logger.Debugf("configBlock height:%v", heightKey)

	return blockhelper.GetBlockByHeightKey(b.dbHandle, b.cache, heightKey, true, b.logger, b.handleTx)
}

// GetLastConfigBlockHeight returns the last config block height.
//
//	@Description:
//	@receiver b
//	@return uint64
//	@return error
func (b *BlockKvDB) GetLastConfigBlockHeight() (uint64, error) {
	heightKey, err := blockhelper.GetFromCacheFirst([]byte(blockhelper.LastConfigBlockNumKey),
		b.dbHandle, b.cache, b.logger)
	if err != nil {
		return math.MaxUint64, err
	}
	b.logger.Debugf("configBlock height:%v", heightKey)
	return blockhelper.DecodeBlockNumKey(b.dbHandle.GetDbType(), heightKey), nil
}

// GetFilteredBlock returns a filtered block given its block height, or return nil if none exists.
//
//	@Description:
//	@receiver b
//	@param height
//	@return *storePb.SerializedBlock
//	@return error
func (b *BlockKvDB) GetFilteredBlock(height uint64) (*storePb.SerializedBlock, error) {
	heightKey := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), height)
	vBytes, err := blockhelper.GetFromCacheFirst(heightKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	} else if vBytes == nil {
		return nil, nil
	}
	var blockStoreInfo storePb.SerializedBlock
	err = proto.Unmarshal(vBytes, &blockStoreInfo)
	if err != nil {
		return nil, err
	}
	return &blockStoreInfo, nil
}

// GetLastSavepoint returns the last block height
//
//	@Description:
//	@receiver b
//	@return uint64
//	@return error
func (b *BlockKvDB) GetLastSavepoint() (uint64, error) {
	vBytes, err := blockhelper.GetFromCacheFirst([]byte(blockhelper.LastBlockNumKeyStr),
		b.dbHandle, b.cache, b.logger)
	if err != nil {
		return 0, err
	} else if vBytes == nil {
		return 0, nil
	}

	num := binary.BigEndian.Uint64(vBytes)
	return num, nil
}

// GetBlockByTx returns a block which contains a tx.
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *commonPb.Block
//	@return error
func (b *BlockKvDB) GetBlockByTx(txId string) (*commonPb.Block, error) {
	txInfo, err := b.getTxInfoOnly(txId)
	if err != nil {
		return nil, err
	}
	if txInfo == nil {
		return nil, nil
	}
	return b.GetBlock(txInfo.BlockHeight)
}

// GetTxHeight retrieves a transaction height by txid, or returns nil if none exists.
//
//	@Description:
//	@receiver b
//	@param txId
//	@return uint64
//	@return error
func (b *BlockKvDB) GetTxHeight(txId string) (uint64, error) {
	blockTxIdKey := blockhelper.ConstructBlockTxIDKey(txId)
	txIdBlockInfoBytes, err := blockhelper.GetFromCacheFirst(blockTxIdKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return 0, err
	}

	if txIdBlockInfoBytes == nil {
		return 0, errValueNotFound
	}
	height, _, _, _, err := blockhelper.ParseTxIdBlockInfo(txIdBlockInfoBytes)
	//if err != nil {
	//	b.logger.Infof("chain[%s]: put block[%d] hash[%x] (txs:%d bytes:%d), ",
	//		block.Header.ChainId, block.Header.BlockHeight, block.Header.BlockHash, len(block.Txs), len(blockBytes))
	//}
	return height, err
}

// GetTx retrieves a transaction by txid, or returns nil if none exists.
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *commonPb.Transaction
//	@return error
func (b *BlockKvDB) GetTx(txId string) (*commonPb.Transaction, error) {
	txIdKey := blockhelper.ConstructTxIDKey(txId)
	vBytes, err := blockhelper.GetFromCacheFirst(txIdKey, b.dbHandle, b.cache, b.logger)
	return b.handleTx(txId, vBytes, err)
}

// handleTx retrieves a transaction by txid, or returns nil if none exists.
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *commonPb.Transaction
//	@return error
func (b *BlockKvDB) handleTx(txId string, vBytes []byte, err error) (*commonPb.Transaction, error) {
	if err != nil {
		return nil, err
	} else if len(vBytes) == 0 {
		isArchived, erra := b.TxArchived(txId)
		if erra == nil && isArchived {
			return nil, tbf.ErrArchivedTx
		}

		return nil, nil
	}

	var tx commonPb.Transaction
	err = proto.Unmarshal(vBytes, &tx)
	if err != nil {
		return nil, err
	}

	return &tx, nil
}

// GetTxWithBlockInfo 根据txId获得交易
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *storePb.TransactionStoreInfo
//	@return error
func (b *BlockKvDB) GetTxWithBlockInfo(txId string) (*storePb.TransactionStoreInfo, error) {
	txIdKey := blockhelper.ConstructTxIDKey(txId)
	vBytes, err := blockhelper.GetFromCacheFirst(txIdKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	} else if len(vBytes) == 0 {
		isArchived, erra := b.TxArchived(txId)
		if erra == nil && isArchived {
			return nil, tbf.ErrArchivedTx
		}
		return nil, nil
	}

	var tx commonPb.Transaction
	err = proto.Unmarshal(vBytes, &tx)
	if err != nil {
		return nil, err
	}
	txInfo, err := b.getTxInfoOnly(txId)
	if err != nil {
		return nil, err
	}
	txInfo.Transaction = &tx
	return txInfo, nil
}

// getTxInfoOnly 获得除Tx之外的其他TxInfo信息
// @Description:
// @receiver b
// @param txId
// @return *storePb.TransactionStoreInfo
// @return error
func (b *BlockKvDB) getTxInfoOnly(txId string) (*storePb.TransactionStoreInfo, error) {
	txIDBlockInfoBytes, err := blockhelper.GetFromCacheFirst(blockhelper.ConstructBlockTxIDKey(txId),
		b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}
	if txIDBlockInfoBytes == nil {
		return nil, nil
	}
	height, blockHash, txIndex, timestamp, err := blockhelper.ParseTxIdBlockInfo(txIDBlockInfoBytes)
	if err != nil {
		return nil, err
	}
	return &storePb.TransactionStoreInfo{
			BlockHeight:    height,
			BlockHash:      blockHash,
			TxIndex:        txIndex,
			BlockTimestamp: timestamp},
		nil
}

// GetTxInfoOnly 获得除Tx之外的其他TxInfo信息
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *storePb.TransactionStoreInfo
//	@return error
func (b *BlockKvDB) GetTxInfoOnly(txId string) (*storePb.TransactionStoreInfo, error) {
	return b.getTxInfoOnly(txId)
}

// TxExists returns true if the tx exist, or returns false if none exists.
//
//	@Description:
//	@receiver b
//	@param txId
//	@return bool
//	@return error
func (b *BlockKvDB) TxExists(txId string) (bool, error) {
	txHashKey := blockhelper.ConstructBlockTxIDKey(txId)
	exist, err := b.has(txHashKey)
	if err != nil {
		b.logger.Errorf("check tx exist by txid:[%s] in blockkvdb error:[%s]", txId, err)
		return false, err
	}
	return exist, nil
}

// TxArchived returns true if the tx archived, or returns false.
//
//	@Description:
//	@receiver b
//	@param txId
//	@return bool
//	@return error
func (b *BlockKvDB) TxArchived(txId string) (bool, error) {
	txIdBlockInfoBytes, err := b.dbHandle.Get(blockhelper.ConstructBlockTxIDKey(txId))
	if len(txIdBlockInfoBytes) == 0 {
		b.logger.Infof("get value []byte ,TxArchived txid[%s] txIdBlockInfoBytes[%s]",
			txId, txIdBlockInfoBytes)
	}

	if err != nil {
		return false, err
	}

	if txIdBlockInfoBytes == nil {
		return false, errValueNotFound
	}

	archivedPivot, err := b.GetArchivedPivot()
	if err != nil {
		return false, err
	}
	height, _, _, _, err := blockhelper.ParseTxIdBlockInfo(txIdBlockInfoBytes)
	if err != nil {
		return false, err
	}
	if height <= archivedPivot {
		return true, nil
	}

	return false, nil
}

// GetTxConfirmedTime returns the confirmed time of a given tx
//
//	@Description:
//	@receiver b
//	@param txId
//	@return int64
//	@return error
func (b *BlockKvDB) GetTxConfirmedTime(txId string) (int64, error) {
	txInfo, err := b.getTxInfoOnly(txId)
	if err != nil {
		return 0, err
	}
	if txInfo == nil {
		return 0, nil
	}
	//如果是新版本，有BlockTimestamp。直接返回
	if txInfo.BlockTimestamp > 0 {
		return txInfo.BlockTimestamp, nil
	}
	//从TxInfo拿不到Timestamp，那么就从Block去拿
	heightBytes := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), txInfo.BlockHeight)
	block, err := blockhelper.GetBlockByHeightKey(b.dbHandle, b.cache, heightBytes, true, b.logger, b.handleTx)
	if err != nil {
		return 0, err
	}
	return block.Header.BlockTimestamp, nil
}

// GetDbType add next time
// @Description:
// @receiver b
// @return string
func (b *BlockKvDB) GetDbType() string {
	return b.dbHandle.GetDbType()
}

// Close is used to close database
//
//	@Description:
//	@receiver b
func (b *BlockKvDB) Close() {
	//获得所有信号量，表示没有任何写操作了
	//b.logger.Infof("wait semaphore[%d]", b.worker)
	//err := b.workersSemaphore.Acquire(context.Background(), b.worker)
	//if err != nil {
	//	b.logger.Errorf("semaphore Acquire error:%s", err)
	//}
	b.logger.Info("close block kv db")
	b.dbHandle.Close()
	b.cache.Clear()
}

// writeBatch 将batch中的区块，写入db中
// @Description:
// @receiver b
// @param blockHeight
// @param batch
// @return error
func (b *BlockKvDB) writeBatch(blockHeight uint64, batch protocol.StoreBatcher) error {
	isLevelDB := b.dbHandle.GetDbType() == leveldbprovider.DbType_Leveldb
	startWriteBatchTime := utils.CurrentTimeMillisSeconds()
	lastKey := []byte(blockhelper.LastBlockNumKeyStr)
	value, err := batch.Get(lastKey)
	if err != nil {
		return err
	}
	batch.Remove(lastKey)

	if err = b.dbHandle.WriteBatch(batch, !isLevelDB); err != nil {
		panic(fmt.Sprintf("Error writing blockKvDB : %s", err))
	}

	// write savePoint
	if err = b.dbHandle.Put(lastKey, value); err != nil {
		panic(fmt.Sprintf("Error writing blockKvDB : %s", err))
	}
	endWriteBatchTime := utils.CurrentTimeMillisSeconds()
	b.logger.Infof("write block db, block[%d], current batch cnt: %d, time used: %d",
		blockHeight, batch.Len(), endWriteBatchTime-startWriteBatchTime)
	return nil
}

// has 判断 key 是否存在
// @Description:
// @receiver b
// @param key
// @return bool
// @return error
func (b *BlockKvDB) has(key []byte) (bool, error) {
	//check has from cache
	isDelete, exist := b.cache.Has(string(key))
	if exist {
		return !isDelete, nil
	}
	return b.dbHandle.Has(key)
}

// getNextArchivePivot get next archive pivot
// @Description:
// @receiver b
// @param pivotBlock
// @return uint64
// @return error
func (b *BlockKvDB) getNextArchivePivot(pivotBlock *commonPb.Block) (uint64, error) {
	archivedPivot := pivotBlock.Header.BlockHeight
	for {
		//consider restore height 1 and height 0 block
		//1. height 1: this is a config block, archivedPivot should be 0
		//2. height 1: this is not a config block, archivedPivot should be 0
		//3. height 0: archivedPivot should be 0
		if archivedPivot < 2 {
			archivedPivot = 0
			break
		}

		//we should not get block data only if it is config block
		archivedPivot = archivedPivot - 1
		_, errb := b.GetBlock(archivedPivot)
		if errb == tbf.ErrArchivedBlock {
			//curIsConf = false
			break
		} else if errb != nil {
			return 0, errb
		}
	}
	return archivedPivot, nil
}

// CompactRange fo kvdb compact action
// @Description:
// @return error
func (b *BlockKvDB) CompactRange() error {
	err := b.dbHandle.CompactRange(nil, nil)
	if err != nil {
		b.logger.Warnf("blockdb level compact failed: %v", err)
	}
	return nil
}

// GetBlockMetaIndex not implement
//
//	@Description:
//	@receiver b
//	@param height
//	@return *storePb.StoreInfo
//	@return error
func (b *BlockKvDB) GetBlockMetaIndex(height uint64) (*storePb.StoreInfo, error) {
	//TODO implement me
	panic("implement GetBlockMetaIndex")
}

// GetTxIndex not implement
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *storePb.StoreInfo
//	@return error
func (b *BlockKvDB) GetTxIndex(txId string) (*storePb.StoreInfo, error) {
	//TODO implement me
	panic("implement GetTxIndex")
}

// GetBlockIndex not implement
//
//	@Description:
//	@receiver b
//	@param height
//	@return *storePb.StoreInfo
//	@return error
func (b *BlockKvDB) GetBlockIndex(height uint64) (*storePb.StoreInfo, error) {
	//TODO implement me
	panic("implement GetBlockIndex")
}
