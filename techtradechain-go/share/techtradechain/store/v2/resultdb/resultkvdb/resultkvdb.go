// Package resultkvdb package
package resultkvdb

/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	leveldbprovider "techtradechain.com/techtradechain/store-leveldb/v2"
	"techtradechain.com/techtradechain/store/v2/cache"
	"techtradechain.com/techtradechain/store/v2/conf"
	"techtradechain.com/techtradechain/store/v2/resultdb/resulthelper"
	"techtradechain.com/techtradechain/store/v2/serialization"
	"techtradechain.com/techtradechain/store/v2/types"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
)

// ResultKvDB provider an implementation of `historydb.HistoryDB`
// @Description:
// This implementation provides a key-value based data model
type ResultKvDB struct {
	dbHandle    protocol.DBHandle
	cache       *cache.StoreCacheMgr
	logger      protocol.Logger
	storeConfig *conf.StorageConfig
	sync.Mutex
}

// NewResultKvDB construct ResultKvDB
// @Description:
// @param chainId
// @param handle
// @param logger
// @param storeConfig
// @return *ResultKvDB
func NewResultKvDB(chainId string, handle protocol.DBHandle, logger protocol.Logger,
	storeConfig *conf.StorageConfig) *ResultKvDB {
	return &ResultKvDB{
		dbHandle:    handle,
		cache:       cache.NewStoreCacheMgr(chainId, 10, logger),
		logger:      logger,
		storeConfig: storeConfig,
	}
}

// InitGenesis init genesis block
// @Description:
// @receiver h
// @param genesisBlock
// @return error
func (h *ResultKvDB) InitGenesis(genesisBlock *serialization.BlockWithSerializedInfo) error {
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
func (h *ResultKvDB) CommitBlock(blockInfo *serialization.BlockWithSerializedInfo, isCache bool) error {
	start := time.Now()
	batch, ok := types.NewUpdateBatch().(*types.UpdateBatch)
	if !ok {
		return fmt.Errorf("commit block get batch failed")
	}
	resulthelper.BuildKVBatch(batch, blockInfo, h.logger)

	block := blockInfo.Block
	if isCache {
		h.cache.AddBlock(block.Header.BlockHeight, batch)
	}

	batchDur := time.Since(start)
	err := h.writeBatch(block.Header.BlockHeight, batch)
	if err != nil {
		return err
	}
	writeDur := time.Since(start)
	h.logger.Debugf("chain[%s]: commit block[%d] kv resultdb, time used (batch[%d]:%d, "+
		"write:%d, total:%d)", block.Header.ChainId, block.Header.BlockHeight,
		batch.Len(), batchDur.Milliseconds(), (writeDur - batchDur).Milliseconds(),
		time.Since(start).Milliseconds())
	return nil
}

// ShrinkBlocks archive old blocks rwsets in an atomic operation
// @Description:
// @receiver h
// @param txIdsMap
// @return error
func (h *ResultKvDB) ShrinkBlocks(txIdsMap map[uint64][]string, _ uint64, _ string) error {
	var err error

	for _, txIds := range txIdsMap {
		batch := types.NewUpdateBatch()
		for _, txId := range txIds {
			txRWSetKey := resulthelper.ConstructTxRWSetIDKey(txId)
			batch.Delete(txRWSetKey)
		}
		if err = h.dbHandle.WriteBatch(batch, true); err != nil {
			return err
		}
	}

	go h.compactRange()

	return nil
}

// RestoreBlocks restore block info,put it into kv db,used by archive module
// @Description:
// @receiver h
// @param blockInfos
// @return error
func (h *ResultKvDB) RestoreBlocks(blockInfos []*serialization.BlockWithSerializedInfo) error {
	startTime := utils.CurrentTimeMillisSeconds()
	for i := len(blockInfos) - 1; i >= 0; i-- {
		blockInfo := blockInfos[i]

		//check whether block can be archived
		if utils.IsConfBlock(blockInfo.Block) {
			h.logger.Infof("skip store conf block: [%d]", blockInfo.Block.Header.BlockHeight)
			continue
		}

		txRWSets := blockInfo.TxRWSets
		rwsetData := blockInfo.SerializedTxRWSets
		batch := types.NewUpdateBatch()
		for index, txRWSet := range txRWSets {
			// rwset: txID -> txRWSet
			batch.Put(resulthelper.ConstructTxRWSetIDKey(txRWSet.TxId), rwsetData[index])
		}
		if err := h.dbHandle.WriteBatch(batch, true); err != nil {
			return err
		}
	}

	beforeWrite := utils.CurrentTimeMillisSeconds()

	go h.compactRange()

	writeTime := utils.CurrentTimeMillisSeconds() - beforeWrite
	h.logger.Infof("restore block RWSets from [%d] to [%d] time used (prepare_txs:%d write_batch:%d, total:%d)",
		blockInfos[len(blockInfos)-1].Block.Header.BlockHeight, blockInfos[0].Block.Header.BlockHeight,
		beforeWrite-startTime, writeTime, utils.CurrentTimeMillisSeconds()-startTime)

	return nil
}

// GetTxRWSet  returns an txRWSet for given txId, or returns nil if none exists.
// @Description:
// @receiver h
// @param txId
// @return *commonPb.TxRWSet
// @return error
func (h *ResultKvDB) GetTxRWSet(txId string) (*commonPb.TxRWSet, error) {
	txRWSetKey := resulthelper.ConstructTxRWSetIDKey(txId)
	bytes, err := h.get(txRWSetKey)
	if err != nil {
		return nil, err
	} else if bytes == nil {
		return nil, nil
	}

	var txRWSet commonPb.TxRWSet
	err = proto.Unmarshal(bytes, &txRWSet)
	if err != nil {
		return nil, err
	}
	return &txRWSet, nil
}

// GetRWSetIndex returns the offset of the block in the file
// @Description:
// @receiver h
// @param txId
// @return *storePb.StoreInfo
// @return error
func (h *ResultKvDB) GetRWSetIndex(txId string) (*storePb.StoreInfo, error) {
	panic("not support")
}

// GetLastSavepoint returns the last block height
// @Description:
// @receiver h
// @return uint64
// @return error
func (h *ResultKvDB) GetLastSavepoint() (uint64, error) {
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
func (h *ResultKvDB) Close() {
	h.logger.Info("close result kv db")
	h.dbHandle.Close()
	h.cache.Clear()
}

// writeBatch write batch into db,and del cache
// @Description:
// @receiver h
// @param blockHeight
// @param batch
// @return error
func (h *ResultKvDB) writeBatch(blockHeight uint64, batch protocol.StoreBatcher) error {
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
	//			panic(fmt.Sprintf("Error writing result db: %s", err))
	//		}
	//	}(i)
	//}
	//wg.Wait()
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
		panic(fmt.Sprintf("Error writing result db: %s", err))
	}

	//db committed, clean cache
	h.cache.DelBlock(blockHeight)
	//writeDur := time.Since(start)
	//h.logger.Infof("write result db, block[%d], time used: (batchSplit[%d]:%d, "+
	//	"write:%d, total:%d)", blockHeight, len(batches), batchDur.Milliseconds(),
	//	(writeDur - batchDur).Milliseconds(), time.Since(start).Milliseconds())

	return nil
}

// get get value from cache,not found get it from kv db
// @Description:
// @receiver h
// @param key
// @return []byte
// @return error
func (h *ResultKvDB) get(key []byte) ([]byte, error) {
	//get from cache
	value, exist := h.cache.Get(string(key))
	if exist {
		return value, nil
	}
	//get from database
	return h.dbHandle.Get(key)
}

// compactRange compact underlying kv db
// @Description:
// @receiver h
func (h *ResultKvDB) compactRange() {
	//trigger level compact
	for i := 1; i <= 1; i++ {
		h.logger.Infof("Do %dst time CompactRange", i)
		if err := h.dbHandle.CompactRange(nil, nil); err != nil {
			h.logger.Warnf("resultdb level compact failed: %v", err)
		}
		//time.Sleep(2 * time.Second)
	}
}
