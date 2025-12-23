/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package blockfiledb

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	leveldbprovider "techtradechain.com/techtradechain/store-leveldb/v2"
	"techtradechain.com/techtradechain/store/v2/binlog"
	"techtradechain.com/techtradechain/store/v2/blockdb/blockhelper"
	"techtradechain.com/techtradechain/store/v2/cache"
	"techtradechain.com/techtradechain/store/v2/conf"
	"techtradechain.com/techtradechain/store/v2/serialization"
	"techtradechain.com/techtradechain/store/v2/types"
	tbf "techtradechain.com/techtradechain/store/v2/types/blockfile"
	su "techtradechain.com/techtradechain/store/v2/utils"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
	"golang.org/x/sync/semaphore"
)

var (
	errValueNotFound = errors.New("value not found")
	errGetBatchPool  = errors.New("get updatebatch error")
)

// BlockFileDB provider an implementation of `dbHandle.BlockDB`
// This implementation provides a key-value based data model
type BlockFileDB struct {
	sync.Mutex

	dbHandle         protocol.DBHandle
	workersSemaphore *semaphore.Weighted
	worker           int64
	cache            *cache.StoreCacheMgr
	archivedPivot    uint64
	logger           protocol.Logger
	batchPools       []*sync.Pool
	storeConfig      *conf.StorageConfig
	fileStore        binlog.BinLogger
}

// NewBlockFileDB construct BlockFileDB
//
//	@Description:
//	@param chainId
//	@param dbHandle
//	@param logger
//	@param storeConfig
//	@param fileStore
//	@return *BlockFileDB
func NewBlockFileDB(chainId string, dbHandle protocol.DBHandle, logger protocol.Logger,
	storeConfig *conf.StorageConfig, fileStore binlog.BinLogger) *BlockFileDB {
	nWorkers := int64(runtime.NumCPU())
	b := &BlockFileDB{
		dbHandle:         dbHandle,
		worker:           nWorkers,
		workersSemaphore: semaphore.NewWeighted(nWorkers),
		cache:            cache.NewStoreCacheMgr(chainId, storeConfig.BlockWriteBufferSize, logger),
		archivedPivot:    0,
		logger:           logger,
		batchPools:       make([]*sync.Pool, 0, 3),
		storeConfig:      storeConfig,
		fileStore:        fileStore,
	}

	// b.batchPools[0] for commit block update batch
	b.batchPools = append(b.batchPools, types.NewSyncPoolUpdateBatch())
	// b.batchPools[1] for shrink block update batch
	b.batchPools = append(b.batchPools, types.NewSyncPoolUpdateBatch())
	// b.batchPools[2] for restore block update batch
	b.batchPools = append(b.batchPools, types.NewSyncPoolUpdateBatch())

	if len(b.batchPools) < 3 {
		panic("block batch should more than 3 batches sync.Pool")
	}

	return b
}

//// GetCache add next time
//// @Description:
//// @receiver b
//// @return *cache.StoreCacheMgr
//func (b *BlockFileDB) GetCache() *cache.StoreCacheMgr {
//	return b.cache.DelBlock()
//}

// InitGenesis init genesis block
//
//	@Description:
//	@receiver b
//	@param genesisBlock
//	@return error
func (b *BlockFileDB) InitGenesis(genesisBlock *serialization.BlockWithSerializedInfo) error {
	//update Cache
	err := b.CommitBlock(genesisBlock, true)
	if err != nil {
		return err
	}
	//update BlockFileDB
	return b.CommitBlock(genesisBlock, false)
}

// CommitBlock commits the block and the corresponding rwsets in an atomic operation
//
//	@Description:
//	@receiver b
//	@param blockInfo
//	@param isCache
//	@return error
func (b *BlockFileDB) CommitBlock(blockInfo *serialization.BlockWithSerializedInfo, isCache bool) error {
	//原则上，写db失败，重试一定次数后，仍然失败，panic

	//如果是更新cache，则 直接更新 然后返回，不写 dbHandle
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
func (b *BlockFileDB) CommitCache(blockInfo *serialization.BlockWithSerializedInfo) error {
	//如果是更新cache，则 直接更新 然后返回，不写 dbHandle
	//从对象池中取一个对象,并重置
	start := time.Now()
	batch, ok := b.batchPools[0].Get().(*types.UpdateBatch)
	if !ok {
		b.logger.Errorf("chain[%s]: blockInfo[%d] get updatebatch error",
			blockInfo.Block.Header.ChainId, blockInfo.Block.Header.BlockHeight)
		return errGetBatchPool
	}
	batch.ReSet()
	dbType := b.dbHandle.GetDbType()
	//batch := types.NewUpdateBatch()
	// 1. last blockInfo height
	block := blockInfo.Block
	batch.Put([]byte(blockhelper.LastBlockNumKeyStr), blockhelper.EncodeBlockNum(block.Header.BlockHeight))

	// 2. height-> blockInfo
	if blockInfo.Index == nil || blockInfo.MetaIndex == nil {
		return fmt.Errorf("blockInfo.Index and blockInfo.MetaIndex must not nil while block rfile db enabled")
	}
	blockIndexKey := blockhelper.ConstructBlockIndexKey(dbType, block.Header.BlockHeight)
	blockIndexInfo, err := proto.Marshal(blockInfo.Index)
	if err != nil {
		return err
	}
	batch.Put(blockIndexKey, blockIndexInfo)
	b.logger.Debugf("put block[%d] rfile index:%v", block.Header.BlockHeight, blockInfo.Index)
	//save block meta index to db
	metaIndexKey := blockhelper.ConstructBlockMetaIndexKey(dbType, block.Header.BlockHeight)
	metaIndexInfo := su.ConstructDBIndexInfo(blockInfo.Index, blockInfo.MetaIndex.Offset,
		blockInfo.MetaIndex.ByteLen)
	batch.Put(metaIndexKey, metaIndexInfo)

	// 4. hash-> height
	hashKey := blockhelper.ConstructBlockHashKey(b.dbHandle.GetDbType(), block.Header.BlockHash)
	batch.Put(hashKey, blockhelper.EncodeBlockNum(block.Header.BlockHeight))

	// 4. txid -> tx,  txid -> blockHeight
	// 5. Concurrency batch Put
	//并发更新cache,ConcurrencyMap,提升并发更新能力
	//groups := len(blockInfo.SerializedContractEvents)
	wg := &sync.WaitGroup{}
	wg.Add(len(blockInfo.SerializedTxs))

	for index, txBytes := range blockInfo.SerializedTxs {
		go func(index int, txBytes []byte, batch protocol.StoreBatcher, wg *sync.WaitGroup) {
			defer wg.Done()
			tx := blockInfo.Block.Txs[index]

			// if block rfile db disable, save tx data to db
			txFileIndex := blockInfo.TxsIndex[index]

			// 把tx的地址写入数据库
			blockTxIdKey := blockhelper.ConstructBlockTxIDKey(tx.Payload.TxId)
			txBlockInf := blockhelper.ConstructFBTxIDBlockInfo(block.Header.BlockHeight, block.Header.BlockHash, uint32(index),
				block.Header.BlockTimestamp, blockInfo.Index, txFileIndex)

			batch.Put(blockTxIdKey, txBlockInf)
			//b.logger.Debugf("put tx[%s] rfile index:%v", tx.Payload.TxId, txFileIndex)
		}(index, txBytes, batch, wg)
	}

	wg.Wait()

	// last configBlock height
	if utils.IsConfBlock(block) || block.Header.BlockHeight == 0 {
		batch.Put([]byte(blockhelper.LastConfigBlockNumKey), blockhelper.EncodeBlockNum(block.Header.BlockHeight))
		b.logger.Infof("chain[%s]: commit config blockInfo[%d]", block.Header.ChainId, block.Header.BlockHeight)
	}

	// 6. 增加cache,注意这个batch放到cache中了，正在使用，不能放到batchPool中
	b.cache.AddBlock(block.Header.BlockHeight, batch)

	b.logger.Debugf("chain[%s]: commit cache block[%d] blockfiledb, batch[%d], time used: %d",
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
func (b *BlockFileDB) CommitDB(blockInfo *serialization.BlockWithSerializedInfo) error {
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

	//3. Delete block from Cache, put batch to batchPools
	//把cacheBatch 从Cache 中删除
	b.cache.DelBlock(block.Header.BlockHeight)

	//再把cacheBatch放回到batchPool 中 (注意这两步前后不能反了)
	b.batchPools[0].Put(cacheBatch)
	writeDur := time.Since(start)

	b.logger.Debugf("chain[%s]: commit block[%d] kv blockfiledb, time used (batch[%d]:%d, "+
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
func (b *BlockFileDB) GetArchivedPivot() (uint64, error) {
	heightBytes, err := b.dbHandle.Get([]byte(blockhelper.ArchivedPivotKey))
	if err != nil {
		if strings.Contains(err.Error(), "leveldb: not found") {
			b.archivedPivot = 0
			err = nil
		}
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

// IsArchivedPivotKeyZeroStatus get archived pivot status
//
//	@Description: if the archive never occur , return true, else return false
//	this interface is used to distinguish the chain contains the block 0 or only lacks block 0
//	@return bool
func (b *BlockFileDB) IsArchivedPivotKeyZeroStatus() bool {
	archivePivotValue, err := b.dbHandle.Get([]byte(blockhelper.ArchivedPivotKey))
	if err != nil {
		//if errors.Is(err, leveldb.ErrNotFound) || errors.Is(err, badger.ErrKeyNotFound) {
		if strings.Contains(err.Error(), "not found") {
			return true
		}
		b.logger.Warnf("b.dbHandle.Get  ArchivedPivotKey error:%s", err)
	}
	if archivePivotValue == nil {
		return true
	}
	return false
}

// ShrinkBlocks remove ranged txid--SerializedTx from kvdb
// @Description:
// @receiver b
// @param _
// @param _
// @param fdbPath
// @return map[uint64][]string
// @return error
func (b *BlockFileDB) ShrinkBlocks(height, _ uint64, bfdbPath string) (map[uint64][]string, error) {
	// prepare archive batch pool
	batch, ok := b.batchPools[1].Get().(*types.UpdateBatch)
	if !ok {
		return nil, fmt.Errorf("archive get shrink update batch failed")
	}
	batch.ReSet()

	// read index file content and delete all kvdb index
	indexPath := path.Join(bfdbPath, tbf.ArchivingPath,
		fmt.Sprintf("%s%s", tbf.Uint64ToSegmentName(height), tbf.ArchivingBlockFileName))
	bytes, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}

	var kvPairs []*tbf.IndexPair
	err = json.Unmarshal(bytes, &kvPairs)
	if err != nil {
		return nil, err
	}

	nBatch := types.IndexPairsToBatch(kvPairs, batch, b.logger)
	apk := []byte(blockhelper.ArchivedPivotKey)
	ap, err1 := nBatch.Get(apk)
	if err1 != nil || ap == nil {
		return nil, fmt.Errorf("archived pivot key should not exist in shrink batch, err: %v", err1)
	}
	if err1 = b.dbHandle.Put(apk, ap); err1 != nil {
		b.logger.Errorf("shrink block write archived pivot failed, in file: %s, err: %v", indexPath, err1)
		return nil, err1
	}
	nBatch.Remove(apk)
	batches := nBatch.SplitBatch(b.storeConfig.WriteBatchSize * 5)
	for i, bth := range batches {
		if err1 = b.dbHandle.WriteBatch(bth, true); err1 != nil {
			b.logger.Errorf("shrink block write batch[%d/%d][%d] failed, err: %v",
				i, len(batches), bth.Len(), err1)
			return nil, err1
		}
		b.logger.Debugf("shrink block write batch[%d/%d][%d] finished", i, len(batches), bth.Len())
	}

	b.batchPools[1].Put(batch)

	// delete archived index file
	return nil, os.RemoveAll(indexPath)
}

// RestoreBlocks restore block data from outside to kvdb: txid--SerializedTx
//
//	@Description:
//	@receiver b
//	@param blockInfos
//	@return error
func (b *BlockFileDB) RestoreBlocks(blockInfos []*serialization.BlockWithSerializedInfo) error {
	// prepare archive batch pool
	dbType := b.dbHandle.GetDbType()
	for _, blkInfo := range blockInfos {
		if blkInfo.Meta == nil || blkInfo.Meta.Header == nil {
			return fmt.Errorf("blkInfo is invalidate: %v", blkInfo)
		}
		batch, ok := b.batchPools[2].Get().(*types.UpdateBatch)
		if !ok {
			return fmt.Errorf("archive get restore update batch failed")
		}
		batch.ReSet()

		header := blkInfo.Meta.Header
		blockIndexInfo, err := proto.Marshal(blkInfo.Index)
		if err != nil {
			return err
		}
		batch.Put(blockhelper.ConstructBlockIndexKey(dbType, header.BlockHeight), blockIndexInfo)

		metaIndexInfo := su.ConstructDBIndexInfo(blkInfo.Index, blkInfo.MetaIndex.Offset, blkInfo.MetaIndex.ByteLen)
		batch.Put(blockhelper.ConstructBlockMetaIndexKey(dbType, header.BlockHeight), metaIndexInfo)

		for k, txId := range blkInfo.Meta.TxIds {
			txBlockInf := blockhelper.ConstructFBTxIDBlockInfo(header.BlockHeight, header.BlockHash,
				uint32(k), header.BlockTimestamp, blkInfo.Index, blkInfo.TxsIndex[k])
			batch.Put(blockhelper.ConstructBlockTxIDKey(txId), txBlockInf)
		}

		batches := batch.SplitBatch(b.storeConfig.WriteBatchSize * 5)
		for i, bth := range batches {
			if err1 := b.dbHandle.WriteBatch(bth, true); err1 != nil {
				b.logger.Errorf("restore block write batch[%d/%d][%d] failed, err: %v",
					i, len(batches), bth.Len(), err1)
				return err1
			}
			b.logger.Debugf("restore block write batch[%d/%d][%d] finished", i, len(batches), bth.Len())
		}

		bh := uint64(0)
		apk := []byte(blockhelper.ArchivedPivotKey)
		if header.BlockHeight > 0 {
			bh = header.BlockHeight - 1
		}
		if err = b.dbHandle.Put(apk, blockhelper.ConstructBlockNumKey(dbType, bh)); err != nil {
			return fmt.Errorf("archived pivot key [%d] save failed, err: %v", bh, err)
		}
		b.batchPools[2].Put(batch)
	}
	return nil
}

// BlockExists returns true if the block hash exist, or returns false if none exists.
//
//	@Description:
//	@receiver b
//	@param blockHash
//	@return bool
//	@return error
func (b *BlockFileDB) BlockExists(blockHash []byte) (bool, error) {
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
func (b *BlockFileDB) GetBlockByHash(blockHash []byte) (*commonPb.Block, error) {
	hashKey := blockhelper.ConstructBlockHashKey(b.dbHandle.GetDbType(), blockHash)
	heightBytes, err := blockhelper.GetFromCacheFirst(hashKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}
	height := blockhelper.DecodeBlockNum(heightBytes)
	return b.GetBlock(height)
}

// GetHeightByHash returns a block height given its hash, or returns nil if none exists.
//
//	@Description:
//	@receiver b
//	@param blockHash
//	@return uint64
//	@return error
func (b *BlockFileDB) GetHeightByHash(blockHash []byte) (uint64, error) {
	hashKey := blockhelper.ConstructBlockHashKey(b.dbHandle.GetDbType(), blockHash)
	heightBytes, err := blockhelper.GetFromCacheFirst(hashKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return 0, err
	}

	if heightBytes == nil {
		b.logger.Warnf("get blockHash: %s empty", hex.EncodeToString(blockHash))
		return 0, errValueNotFound
	}

	return blockhelper.DecodeBlockNum(heightBytes), nil
}

// GetBlockHeaderByHeight returns a block header by given its height, or returns nil if none exists.
// bfdb archive will remove block metadata
//
//	@Description:
//	@receiver b
//	@param height
//	@return *commonPb.BlockHeader
//	@return error
func (b *BlockFileDB) GetBlockHeaderByHeight(height uint64) (*commonPb.BlockHeader, error) {
	storeInfo, err := b.GetBlockMetaIndex(height)
	if err != nil {
		return nil, err
	}
	if storeInfo == nil {
		return nil, nil
	}
	vBytes, err := b.fileStore.ReadFileSection(storeInfo, 0)
	if err != nil {
		return nil, err
	}
	var blockStoreInfo storePb.SerializedBlock
	err = proto.Unmarshal(vBytes, &blockStoreInfo)
	if err != nil {
		b.logger.Warnf("get block header by height[%d] from file unmarshal block:", height, err)
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
func (b *BlockFileDB) GetBlock(height uint64) (*commonPb.Block, error) {
	// if block rfile db is enabled and db is kvdb, we will get data from block rfile db
	index, err := b.GetBlockIndex(height)
	if err != nil {
		if err == tbf.ErrArchivedBlock {
			heightBytes := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), height)
			blk, _ := blockhelper.GetBlockByHeightKey(b.dbHandle, b.cache, heightBytes, true,
				b.logger, blockhelper.HandleFDKVTx)
			if blk != nil {
				return blk, nil
			}
			return nil, tbf.ErrArchivedBlock
		}
		return nil, err
	}

	data, _, _, _, _ := b.fileStore.ReadLastSegSection(height+1, false)
	if len(data) == 0 {
		if index == nil {
			b.logger.Infof("get block index by height[%d] from block db is nil", height)
			return nil, nil
		}
		data, err = b.fileStore.ReadFileSection(index, 0)
		if err != nil {
			return nil, err
		}
	}

	brw, err := serialization.DeserializeBlock(data)
	if err != nil {
		b.logger.Warnf("get block[%d] from file deserialize block failed: %v", height, err)
		return nil, err
	}
	if brw.Block == nil {
		b.logger.Infof("get block by height[%d] deserializeBlock is nil", height)
	}

	return brw.Block, nil
}

// GetLastBlock returns the last block.
//
//	@Description:
//	@receiver b
//	@return *commonPb.Block
//	@return error
func (b *BlockFileDB) GetLastBlock() (*commonPb.Block, error) {
	num, err := b.GetLastSavepoint()
	if err != nil {
		return nil, err
	}
	return b.GetBlock(num)
}

// GetLastConfigBlock returns the last config block.
//
//	@Description:
//	@receiver b
//	@return *commonPb.Block
//	@return error
func (b *BlockFileDB) GetLastConfigBlock() (*commonPb.Block, error) {
	height, err := b.GetLastConfigBlockHeight()
	if err != nil {
		return nil, err
	}
	b.logger.Debugf("configBlock height:%v", height)
	return b.GetBlock(height)
}

// GetLastConfigBlockHeight returns the last config block height.
//
//	@Description:
//	@receiver b
//	@return uint64
//	@return error
func (b *BlockFileDB) GetLastConfigBlockHeight() (uint64, error) {
	heightBytes, err := blockhelper.GetFromCacheFirst([]byte(blockhelper.LastConfigBlockNumKey),
		b.dbHandle, b.cache, b.logger)
	if err != nil {
		return math.MaxUint64, err
	}
	b.logger.Debugf("configBlock height:%v", heightBytes)
	return blockhelper.DecodeBlockNum(heightBytes), nil
}

// GetFilteredBlock returns a filtered block given its block height, or return nil if none exists.
//
//	@Description:
//	@receiver b
//	@param height
//	@return *storePb.SerializedBlock
//	@return error
func (b *BlockFileDB) GetFilteredBlock(height uint64) (*storePb.SerializedBlock, error) {
	heightKey := blockhelper.ConstructBlockNumKey(b.dbHandle.GetDbType(), height)
	bytes, err := blockhelper.GetFromCacheFirst(heightKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	} else if bytes == nil {
		// 因为文件存储写索引的时候没有记录这个索引,
		// 所以没地方调用GetFilteredBlock方法,不会走到这里
		isArchived, erra := b.checkBlockAndTxIsArchived(height, nil)
		if erra != nil {
			return nil, erra
		}
		if isArchived {
			return nil, tbf.ErrArchivedBlock
		}
		return nil, nil
	}
	var blockStoreInfo storePb.SerializedBlock
	err = proto.Unmarshal(bytes, &blockStoreInfo)
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
func (b *BlockFileDB) GetLastSavepoint() (uint64, error) {
	vBytes, err := blockhelper.GetFromCacheFirst([]byte(blockhelper.LastBlockNumKeyStr),
		b.dbHandle, b.cache, b.logger)
	if err != nil {
		return 0, err
	} else if vBytes == nil {
		return 0, nil
	}

	return blockhelper.DecodeBlockNum(vBytes), nil
}

// GetBlockByTx returns a block which contains a tx.
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *commonPb.Block
//	@return error
func (b *BlockFileDB) GetBlockByTx(txId string) (*commonPb.Block, error) {
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
func (b *BlockFileDB) GetTxHeight(txId string) (uint64, error) {
	blockTxIdKey := blockhelper.ConstructBlockTxIDKey(txId)
	txIdBlockInfoBytes, err := blockhelper.GetFromCacheFirst(blockTxIdKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return 0, err
	}

	if txIdBlockInfoBytes == nil {
		return 0, errValueNotFound
	}
	height, _, _, _, _, err := blockhelper.ParseFBTxIdBlockInfo(txIdBlockInfoBytes)
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
func (b *BlockFileDB) GetTx(txId string) (*commonPb.Transaction, error) {
	// get tx from block rfile db
	tx := &commonPb.Transaction{}
	index, err := b.GetTxIndex(txId)
	if err != nil {
		if err == tbf.ErrArchivedTx {
			v, errk := blockhelper.GetFromCacheFirst(
				blockhelper.ConstructTxIDKey(txId), b.dbHandle, b.cache, b.logger)
			tx, errk = blockhelper.HandleFDKVTx(txId, v, errk)
			if tx == nil && errk == nil {
				return nil, tbf.ErrArchivedTx
			}
			return tx, errk
		}
		return nil, err
	}
	if index == nil {
		return nil, nil
	}
	data, err := b.fileStore.ReadFileSection(index, 0)
	if err != nil {
		return nil, err
	}

	if err = proto.Unmarshal(data, tx); err != nil {
		b.logger.Warnf("get tx[%s] from file unmarshal block:", txId, err)
		return nil, err
	}
	return tx, nil
}

// GetTxWithBlockInfo add next time
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *storePb.TransactionStoreInfo
//	@return error
func (b *BlockFileDB) GetTxWithBlockInfo(txId string) (*storePb.TransactionStoreInfo, error) {
	txInfo, err := b.getTxInfoOnly(txId)
	if err != nil {
		return nil, err
	}
	if txInfo == nil { //查不到对应的Tx，返回nil,nil
		return nil, nil
	}
	tx, err := b.GetTx(txId)
	if err != nil {
		return nil, err
	}
	txInfo.Transaction = tx
	return txInfo, nil
}

// getTxInfoOnly 获得除Tx之外的其他TxInfo信息
// @Description:
// @receiver b
// @param txId
// @return *storePb.TransactionStoreInfo
// @return error
func (b *BlockFileDB) getTxInfoOnly(txId string) (*storePb.TransactionStoreInfo, error) {
	txIDBlockInfoBytes, err := blockhelper.GetFromCacheFirst(blockhelper.ConstructBlockTxIDKey(txId),
		b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}
	if txIDBlockInfoBytes == nil {
		return nil, nil
	}
	txi := &storePb.TransactionStoreInfo{}
	err = txi.Unmarshal(txIDBlockInfoBytes)
	return txi, err
}

// GetTxInfoOnly 获得除Tx之外的其他TxInfo信息
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *storePb.TransactionStoreInfo
//	@return error
func (b *BlockFileDB) GetTxInfoOnly(txId string) (*storePb.TransactionStoreInfo, error) {
	return b.getTxInfoOnly(txId)
}

// TxExists returns true if the tx exist, or returns false if none exists.
//
//	@Description:
//	@receiver b
//	@param txId
//	@return bool
//	@return error
func (b *BlockFileDB) TxExists(txId string) (bool, error) {
	txHashKey := blockhelper.ConstructBlockTxIDKey(txId)
	exist, err := b.has(txHashKey)
	if err != nil {
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
func (b *BlockFileDB) TxArchived(txId string) (bool, error) {
	vBytes, err := b.dbHandle.Get(blockhelper.ConstructBlockTxIDKey(txId))
	if len(vBytes) == 0 {
		b.logger.Infof("get value []byte ,TxArchived txid[%s] vBytes[%s]",
			txId, vBytes)
	}
	if err != nil {
		return false, err
	}
	if vBytes == nil {
		return false, errValueNotFound
	}

	height, _, _, _, txIndex, err := blockhelper.ParseFBTxIdBlockInfo(vBytes)
	if err != nil {
		return false, err
	}
	return b.checkBlockAndTxIsArchived(height, txIndex)
}

// GetTxConfirmedTime returns the confirmed time of a given tx
//
//	@Description:
//	@receiver b
//	@param txId
//	@return int64
//	@return error
func (b *BlockFileDB) GetTxConfirmedTime(txId string) (int64, error) {
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
	block, err := b.GetBlockMeta(txInfo.BlockHeight)
	if err != nil {
		return 0, err
	}
	if block == nil || block.Header == nil {
		return 0, nil
	}
	return block.Header.BlockTimestamp, nil
}

// GetBlockIndex returns the offset of the block in the rfile
//
//	@Description:
//	@receiver b
//	@param height
//	@return *storePb.StoreInfo
//	@return error
func (b *BlockFileDB) GetBlockIndex(height uint64) (*storePb.StoreInfo, error) {
	b.logger.Debugf("get block[%d] index", height)
	indexKey := blockhelper.ConstructBlockIndexKey(b.dbHandle.GetDbType(), height)
	vByte, err := blockhelper.GetFromCacheFirst(indexKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}
	vIndex, err1 := su.DecodeValueToIndex(vByte)
	if err1 == nil {
		b.logger.Debugf("get block[%d] index: %s", height, vIndex.String())
	}
	isArchived, erra := b.checkBlockAndTxIsArchived(height, vIndex)
	if erra != nil {
		return nil, erra
	}
	if isArchived {
		return nil, tbf.ErrArchivedBlock
	}
	return vIndex, err1
}

// GetBlockMeta add next time
//
//	@Description:
//	@receiver b
//	@param height
//	@return *storePb.SerializedBlock
//	@return error
func (b *BlockFileDB) GetBlockMeta(height uint64) (*storePb.SerializedBlock, error) {
	si, err := b.GetBlockMetaIndex(height)
	if err != nil {
		return nil, err
	}
	if si == nil {
		return nil, nil
	}
	data, err := b.fileStore.ReadFileSection(si, 0)
	if err != nil {
		return nil, err
	}
	bm := &storePb.SerializedBlock{}
	err = bm.Unmarshal(data)
	if err != nil {
		b.logger.Warnf("get block[%d] meta from file unmarshal block:", height, err)
		return nil, err
	}
	return bm, nil
}

// GetBlockMetaIndex returns the offset of the block in the rfile
//
//	@Description:
//	@receiver b
//	@param height
//	@return *storePb.StoreInfo
//	@return error
func (b *BlockFileDB) GetBlockMetaIndex(height uint64) (*storePb.StoreInfo, error) {
	b.logger.Debugf("get block[%d] meta index", height)
	indexKey := blockhelper.ConstructBlockMetaIndexKey(b.dbHandle.GetDbType(), height)
	index, err := blockhelper.GetFromCacheFirst(indexKey, b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}
	vIndex, err1 := su.DecodeValueToIndex(index)
	if err1 == nil {
		b.logger.Debugf("get block[%d] meta index: %s", height, vIndex.String())
	}
	isArchived, isArchivedErr := b.checkBlockAndTxIsArchived(height, vIndex)
	if isArchivedErr != nil {
		return nil, isArchivedErr
	}
	if isArchived {
		return nil, tbf.ErrArchivedBlock
	}
	return vIndex, err1
}

// GetTxIndex returns the offset of the transaction in the rfile
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *storePb.StoreInfo
//	@return error
func (b *BlockFileDB) GetTxIndex(txId string) (*storePb.StoreInfo, error) {
	b.logger.Debugf("get rwset txId: %s", txId)
	vBytes, err := blockhelper.GetFromCacheFirst(blockhelper.ConstructBlockTxIDKey(txId),
		b.dbHandle, b.cache, b.logger)
	if err != nil {
		return nil, err
	}
	if len(vBytes) == 0 {
		return nil, nil //not found
	}
	height, _, _, _, txIndex, err := blockhelper.ParseFBTxIdBlockInfo(vBytes)
	isArchived, erra := b.checkBlockAndTxIsArchived(height, txIndex)
	if erra != nil {
		return nil, erra
	}
	if isArchived {
		return nil, tbf.ErrArchivedTx
	}
	b.logger.Debugf("read tx[%s] rfile index get:%v", txId, txIndex)
	return txIndex, err
}

// GetDbType add next time
// @Description:
// @receiver b
// @return string
func (b *BlockFileDB) GetDbType() string {
	return b.dbHandle.GetDbType()
}

// CompactRange fo kvdb compact action
// @Description:
// @return error
func (b *BlockFileDB) CompactRange() error {
	err := b.dbHandle.CompactRange(nil, nil)
	if err != nil {
		b.logger.Warnf("blockdb level compact failed: %v", err)
	}
	return nil
}

// Close is used to close database
//
//	@Description:
//	@receiver b
func (b *BlockFileDB) Close() {
	//获得所有信号量，表示没有任何写操作了
	//b.logger.Infof("wait semaphore[%d]", b.worker)
	//err := b.workersSemaphore.Acquire(context.Background(), b.worker)
	//if err != nil {
	//	b.logger.Errorf("semaphore Acquire error:%s", err)
	//}
	b.logger.Info("close block file db")
	b.dbHandle.Close()
	b.cache.Clear()
}

// writeBatch add next time
// @Description:
// @receiver b
// @param blockHeight
// @param batch
// @return error
func (b *BlockFileDB) writeBatch(blockHeight uint64, batch protocol.StoreBatcher) error {
	start := time.Now()
	isLevelDB := b.dbHandle.GetDbType() == leveldbprovider.DbType_Leveldb
	//remove savePoint from batch
	lastKey := []byte(blockhelper.LastBlockNumKeyStr)
	value, err := batch.Get(lastKey)
	if err != nil {
		return err
	}
	batch.Remove(lastKey)

	batches := batch.SplitBatch(b.storeConfig.WriteBatchSize)
	batchDur := time.Since(start)

	wg := &sync.WaitGroup{}
	wg.Add(len(batches))
	for i := 0; i < len(batches); i++ {
		go func(index int) {
			defer wg.Done()
			if err = b.dbHandle.WriteBatch(batches[index], !isLevelDB); err != nil {
				panic(fmt.Sprintf("Error writing block rfile db: %s", err))
			}
		}(i)
	}
	wg.Wait()

	// write savePoint
	if err = b.dbHandle.Put(lastKey, value); err != nil {
		panic(fmt.Sprintf("Error writing block rfile db: %s", err))
	}

	writeDur := time.Since(start)

	b.logger.Infof("write block rfile db, block[%d], time used: (batchSplit[%d]:%d, "+
		"write:%d, total:%d)", blockHeight, len(batches), batchDur.Milliseconds(),
		(writeDur - batchDur).Milliseconds(), time.Since(start).Milliseconds())
	return nil

	//startWriteBatchTime := utils.CurrentTimeMillisSeconds()
	//err := b.dbHandle.WriteBatch(batch, true)
	//endWriteBatchTime := utils.CurrentTimeMillisSeconds()
	//b.logger.Debugf("write block db, block[%d], current batch cnt: %d, time used: %d",
	//	blockHeight, batch.Len(), endWriteBatchTime-startWriteBatchTime)
	//return err
}

// has first from cache;if not found,from db
// @Description:
// @receiver b
// @param key
// @return bool
// @return error
func (b *BlockFileDB) has(key []byte) (bool, error) {
	//check has from cache
	isDelete, exist := b.cache.Has(string(key))
	if exist {
		return !isDelete, nil
	}
	return b.dbHandle.Has(key)
}

// checkBlockAndTxIsArchived 检查区块/tx是否已经被归档
// @Description: 检查该区块/tx是否已经被归档
// @param height 高度
// @param storeInfo 存储信息
// @param checkStore 是否检查存储文件信息
// @param checkop 检查区块operationCheckBlockIsArchived,检查交易operationCheckTxIsArchived
// @return bool
// @return error
func (b *BlockFileDB) checkBlockAndTxIsArchived(height uint64, storeInfo *storePb.StoreInfo) (bool, error) {
	var isArchived bool
	_, _ = b.GetArchivedPivot()
	if b.archivedPivot == 0 {
		return isArchived, nil
	}
	if height > b.archivedPivot {
		return isArchived, nil
	}
	if storeInfo == nil {
		isArchived = true
	} else {
		// 为了兼容"归档中心清理数据小工具"的逻辑
		// 只有使用了清理数据的工具,且查询的区块所在文件已经被删除,且索引仍旧存在才会走到这里
		exist, err := b.fileStore.CheckFileExist(storeInfo)
		if err != nil {
			b.logger.Errorf("checkBlockAndTxIsArchived height [%d] CheckFileExist eror [%s]",
				height, err.Error())
			return isArchived, err
		}
		if !exist {
			isArchived = true
		}
	}
	// 如果代码走到这里isArchived仍然为false
	// 原因是系统更新了archivedPivot,
	// 但是还没有来及删除所查询高度的索引和文件
	// (在控制好unarchivedHeight足够大的情况下,概率较小)
	return isArchived, nil
}
