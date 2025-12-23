// Package resultfiledb package
package resultfiledb

/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"techtradechain.com/techtradechain/common/v2/json"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	leveldbprovider "techtradechain.com/techtradechain/store-leveldb/v2"
	"techtradechain.com/techtradechain/store/v2/binlog"
	"techtradechain.com/techtradechain/store/v2/cache"
	"techtradechain.com/techtradechain/store/v2/conf"
	"techtradechain.com/techtradechain/store/v2/resultdb/resulthelper"
	"techtradechain.com/techtradechain/store/v2/serialization"
	"techtradechain.com/techtradechain/store/v2/types"
	tbf "techtradechain.com/techtradechain/store/v2/types/blockfile"
	su "techtradechain.com/techtradechain/store/v2/utils"
)

// ResultFileDB provider an implementation of `historydb.HistoryDB`
// @Description:
// This implementation provides a key-value based data model
type ResultFileDB struct {
	dbHandle    protocol.DBHandle
	cache       *cache.StoreCacheMgr
	logger      protocol.Logger
	storeConfig *conf.StorageConfig
	filedb      binlog.BinLogger
	batchPools  []*sync.Pool
	sync.Mutex
}

// NewResultFileDB construct ResultFileDB
// @Description:
// @param chainId
// @param handle
// @param logger
// @param storeConfig
// @param fileDB
// @return *ResultFileDB
func NewResultFileDB(chainId string, handle protocol.DBHandle, logger protocol.Logger,
	storeConfig *conf.StorageConfig, fileDB binlog.BinLogger) *ResultFileDB {
	res := &ResultFileDB{
		dbHandle:    handle,
		cache:       cache.NewStoreCacheMgr(chainId, 10, logger),
		logger:      logger,
		storeConfig: storeConfig,
		filedb:      fileDB,
		batchPools:  make([]*sync.Pool, 0, 2),
	}

	// b.batchPools[0] for commit result update batch
	res.batchPools = append(res.batchPools, types.NewSyncPoolUpdateBatch())
	// b.batchPools[1] for shrink result update batch
	res.batchPools = append(res.batchPools, types.NewSyncPoolUpdateBatch())
	// b.batchPools[2] for restore result update batch
	res.batchPools = append(res.batchPools, types.NewSyncPoolUpdateBatch())

	if len(res.batchPools) < 3 {
		panic("result batch should more than 3 batches sync.Pool")
	}

	return res
}

// InitGenesis init genesis block
// @Description:
// @receiver h
// @param genesisBlock
// @return error
func (h *ResultFileDB) InitGenesis(genesisBlock *serialization.BlockWithSerializedInfo) error {
	err := h.CommitBlock(genesisBlock, true)
	if err != nil {
		return err
	}
	return h.CommitBlock(genesisBlock, false)
}

// CommitBlock commits the block rwsets in an atomic operation
// @Description:
// @receiver h
// @param blockInfo
// @param isCache
// @return error
func (h *ResultFileDB) CommitBlock(blockInfo *serialization.BlockWithSerializedInfo, isCache bool) error {
	start := time.Now()

	batch, ok := h.batchPools[0].Get().(*types.UpdateBatch)
	if !ok {
		return fmt.Errorf("archive get shrink update batch failed")
	}
	batch.ReSet()
	defer h.batchPools[0].Put(batch)

	// 1. last block height
	block := blockInfo.Block
	lastBlockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lastBlockNumBytes, block.Header.BlockHeight)
	batch.Put([]byte(resulthelper.ResultDBSavepointKey), lastBlockNumBytes)

	for index, txRWSet := range blockInfo.TxRWSets {
		rwSetIndexKey := resulthelper.ConstructTxRWSetIndexKey(txRWSet.TxId)
		rwIndex := blockInfo.RWSetsIndex[index]
		rwIndexInfo := su.ConstructDBIndexInfo(blockInfo.Index, rwIndex.Offset, rwIndex.ByteLen)
		batch.Put(rwSetIndexKey, rwIndexInfo)
	}

	if isCache {
		// update Cache
		h.cache.AddBlock(block.Header.BlockHeight, batch)
		h.logger.Infof("chain[%s]: commit result file cache block[%d], batch[%d], time used: %d",
			block.Header.ChainId, block.Header.BlockHeight, batch.Len(),
			time.Since(start).Milliseconds())
	}

	err := h.writeBatch(block.Header.BlockHeight, batch)
	if err != nil {
		return err
	}
	return nil
}

// ShrinkBlocks archive old blocks rwSets in an atomic operation
// @Description:
// @receiver h
// @param txIdsMap
// @param height
// @param bfdbPath
// @return error
func (h *ResultFileDB) ShrinkBlocks(_ map[uint64][]string, height uint64, bfdbPath string) error {
	batch, ok := h.batchPools[1].Get().(*types.UpdateBatch)
	if !ok {
		return fmt.Errorf("get batch pool err")
	}
	batch.ReSet()
	defer h.batchPools[1].Put(batch)

	indexPath := path.Join(bfdbPath, tbf.ArchivingPath,
		fmt.Sprintf("%s%s", tbf.Uint64ToSegmentName(height), tbf.ArchivingResultFileName))
	bytes, err := os.ReadFile(indexPath)
	if err != nil {
		return err
	}

	var kvPairs []*tbf.IndexPair
	err = json.Unmarshal(bytes, &kvPairs)
	if err != nil {
		return err
	}

	nBatch := types.IndexPairsToBatch(kvPairs, batch, h.logger)
	batches := nBatch.SplitBatch(h.storeConfig.WriteBatchSize * 5)
	for i, bth := range batches {
		if err1 := h.dbHandle.WriteBatch(bth, true); err1 != nil {
			h.logger.Errorf("shrink result write batch[%d/%d][%d] failed, err: %v",
				i, len(batches), bth.Len(), err1)
			return err1
		}
		h.logger.Debugf("shrink result write batch[%d/%d][%d] finished", i, len(batches), bth.Len())
	}

	// delete archived index file
	return os.RemoveAll(indexPath)
}

// RestoreBlocks not implement
// @Description:
// @receiver h
// @param blockInfos
// @return error
func (h *ResultFileDB) RestoreBlocks(blockInfos []*serialization.BlockWithSerializedInfo) error {
	for _, blkInfo := range blockInfos {
		if blkInfo.Meta == nil || blkInfo.Meta.Header == nil {
			return fmt.Errorf("blkInfo is invalidate: %v", blkInfo)
		}
		batch, ok := h.batchPools[2].Get().(*types.UpdateBatch)
		if !ok {
			return fmt.Errorf("archive get restore update batch failed")
		}
		batch.ReSet()

		if len(blkInfo.TxRWSets) != len(blkInfo.RWSetsIndex) {
			return fmt.Errorf("blkInfo is invalidate: %v", blkInfo)
		}

		for k, rwIndex := range blkInfo.RWSetsIndex {
			rwSetIndexKey := resulthelper.ConstructTxRWSetIndexKey(blkInfo.TxRWSets[k].TxId)
			rwIndexInfo := su.ConstructDBIndexInfo(blkInfo.Index, rwIndex.Offset, rwIndex.ByteLen)
			batch.Put(rwSetIndexKey, rwIndexInfo)
		}

		batches := batch.SplitBatch(h.storeConfig.WriteBatchSize * 5)
		for i, bth := range batches {
			if err1 := h.dbHandle.WriteBatch(bth, true); err1 != nil {
				h.logger.Errorf("restore block write batch[%d/%d][%d] failed, err: %v",
					i, len(batches), bth.Len(), err1)
				return err1
			}
			h.logger.Debugf("restore block write batch[%d/%d][%d] finished", i, len(batches), bth.Len())
		}
		h.batchPools[2].Put(batch)
	}
	return nil
}

// GetTxRWSet returns an txRWSet for given txId, or returns nil if none exists.
// @Description:
// @receiver h
// @param txId
// @return *commonPb.TxRWSet
// @return error
func (h *ResultFileDB) GetTxRWSet(txId string) (*commonPb.TxRWSet, error) {
	sinfo, err := h.GetRWSetIndex(txId)
	if err != nil {
		return nil, err
	}
	if sinfo == nil {
		// rwSet index is empty, archive can make this state,
		// we need to prevent missing queries in the presence of config blocks
		return resulthelper.GetTxRWSetFromKVDB(txId, h.dbHandle, h.cache)
	}
	data, err := h.filedb.ReadFileSection(sinfo, 0)
	if err != nil {
		return nil, err
	}

	rwset := &commonPb.TxRWSet{}
	err = rwset.Unmarshal(data)
	if err != nil {
		h.logger.Warnf("get tx[%s] rwset from file unmarshal block:", txId, err)
		return nil, err
	}
	return rwset, nil
}

// GetRWSetIndex returns the offset of the block in the file
// @Description:
// @receiver h
// @param txId
// @return *storePb.StoreInfo
// @return error
func (h *ResultFileDB) GetRWSetIndex(txId string) (*storePb.StoreInfo, error) {
	// GetRWSetIndex returns the offset of the block in the file
	h.logger.Debugf("get rwset txId: %s", txId)
	index, err := h.get(resulthelper.ConstructTxRWSetIndexKey(txId))
	if err != nil {
		return nil, err
	}

	vIndex, err1 := su.DecodeValueToIndex(index)
	if err1 == nil {
		h.logger.Debugf("get rwset txId: %s, index: %s", txId, vIndex.String())
	}
	return vIndex, err1
}

// GetLastSavepoint returns the last block height
// @Description:
// @receiver h
// @return uint64
// @return error
func (h *ResultFileDB) GetLastSavepoint() (uint64, error) {
	bytes, err := h.get([]byte(resulthelper.ResultDBSavepointKey))
	if err != nil {
		return 0, err
	} else if bytes == nil {
		return 0, nil
	}
	num := binary.BigEndian.Uint64(bytes)
	return num, nil
}

// Close is used to close database
// @Description:
// @receiver h
func (h *ResultFileDB) Close() {
	h.logger.Info("close result file db")
	h.dbHandle.Close()
	h.cache.Clear()
}

// writeBatch save data,delete cache
// @Description:
// @receiver h
// @param blockHeight
// @param batch
// @return error
func (h *ResultFileDB) writeBatch(blockHeight uint64, batch protocol.StoreBatcher) error {
	//start := time.Now()
	//batches := batch.SplitBatch(102400)
	//batchDur := time.Since(start)
	//
	//wg := &sync.WaitGroup{}
	//wg.Add(len(batches))
	//for i := 0; i < len(batches); i++ {
	//	go func(index int) {
	//		defer wg.Done()
	//		if err := h.dbHandle.WriteBatch(batches[index], true); err != nil {
	//			panic(fmt.Sprintf("Error writing block db: %s", err))
	//		}
	//	}(i)
	//}
	//wg.Wait()
	//writeDur := time.Since(start)
	//h.cache.DelBlock(blockHeight)
	//
	//h.logger.Infof("write result file db, block[%d], time used: (batchSplit[%d]:%d, "+
	//	"write:%d, total:%d)", blockHeight, len(batches), batchDur.Milliseconds(),
	//	(writeDur - batchDur).Milliseconds(), time.Since(start).Milliseconds())

	isLevelDB := h.dbHandle.GetDbType() == leveldbprovider.DbType_Leveldb
	lastKey := []byte(resulthelper.ResultDBSavepointKey)
	value, err := batch.Get(lastKey)
	if err != nil {
		return err
	}
	batch.Remove(lastKey)

	if err = h.dbHandle.WriteBatch(batch, !isLevelDB); err != nil {
		panic(fmt.Sprintf("Error writing db: %s", err))
	}

	// write savePoint
	if err = h.dbHandle.Put(lastKey, value); err != nil {
		panic(fmt.Sprintf("Error writing db: %s", err))
	}

	//db committed, clean cache
	h.cache.DelBlock(blockHeight)

	return nil
}

// get get value from cache,not found,get from db
// @Description:
// @receiver h
// @param key
// @return []byte
// @return error
func (h *ResultFileDB) get(key []byte) ([]byte, error) {
	//get from cache
	value, exist := h.cache.Get(string(key))
	if exist {
		return value, nil
	}
	//get from database
	return h.dbHandle.Get(key)
}
