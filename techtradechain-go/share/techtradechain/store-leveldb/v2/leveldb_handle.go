/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package leveldbprovider

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// DbType_Leveldb string "leveldb"
var DbType_Leveldb = "leveldb"

// check LevelDBHandle implement the interface
var _ protocol.DBHandle = (*LevelDBHandle)(nil)

// ErrBatch for check batch type
var ErrBatch = errors.New("get batch from batchPool err")

const (
	defaultBloomFilterBits = 10
	//defaultWriteBatchSize         = 128
	//defaultNoSync                 = false
	//defaultDisableBufferPool      = false
	//defaultCompression            = 1
	//defaultDisableBlockCache      = false
	//defaultBlockCacheCapacity     = 8
	//defaultBlockSize              = 4
	defaultWriteBufferSize = 4 * opt.MiB
	//defaultCompactionTableSize    = 2
	//defaultCompactionTotalSize    = 10
	//defaultWriteL0PauseTrigger    = 12
	//defaultWriteL0SlowdownTrigger = 8
	//defaultCompactionL0Trigger    = 4
)
const (
	//StoreBlockDBDir blockdb folder name
	StoreBlockDBDir = "store_block"
	//StoreStateDBDir statedb folder name
	StoreStateDBDir = "store_state"
	//StoreHistoryDBDir historydb folder name
	StoreHistoryDBDir = "store_history"
	//StoreResultDBDir resultdb folder name
	StoreResultDBDir = "store_result"
)

// LevelDBHandle encapsulated handle to leveldb
type LevelDBHandle struct {
	writeLock      sync.Mutex
	db             *leveldb.DB
	logger         protocol.Logger
	writeBatchSize uint64
	encryptor      crypto.SymmetricKey
	batchPool      sync.Pool
}

// GetWriteBatchSize return writeBatchSize
// @return uint64
func (h *LevelDBHandle) GetWriteBatchSize() uint64 {
	return h.writeBatchSize
}

// NewLevelDBOptions used to build LevelDBHandle
type NewLevelDBOptions struct {
	Config    *LevelDbConfig
	Logger    protocol.Logger
	Encryptor crypto.SymmetricKey
	ChainId   string
	DbFolder  string
}

// NewLevelDBHandle use NewLevelDBOptions to build LevelDBHandle
// @param NewLevelDBOptions config
// @return *LevelDBHandle dbhandler
func NewLevelDBHandle(options *NewLevelDBOptions) *LevelDBHandle {
	dbConfig := options.Config
	logger := options.Logger
	// make db store path
	dbPath := filepath.Join(dbConfig.StorePath, options.ChainId, options.DbFolder)
	// create store path
	err := createDirIfNotExist(dbPath)
	if err != nil {
		panic(fmt.Sprintf("Error create dir %s by leveldbprovider: %s", dbPath, err))
	}
	// create db use the given path
	db, err := leveldb.OpenFile(dbPath, setupOptions(dbConfig))
	if err != nil {
		//断电或者其他意外情况会导致leveldb manifest文件损坏，这里做一次尝试修复
		if db, err = leveldb.RecoverFile(dbPath, setupOptions(dbConfig)); err != nil {
			panic(fmt.Sprintf("Error opening %s by leveldbprovider: %s", dbPath, err))
		}
	}
	logger.Debugf("open leveldb:%s", dbPath)

	return &LevelDBHandle{
		db:        db,
		logger:    logger,
		encryptor: options.Encryptor,
		batchPool: sync.Pool{New: newBatch},
	}
}

// setupOptions set db option
// @param *LevelDbConfig config
// @return *opt.Options levelDb option
func setupOptions(lcfg *LevelDbConfig) *opt.Options {
	dbOpts := &opt.Options{
		WriteBuffer: defaultWriteBufferSize,
	}
	if lcfg.WriteBufferSize > 0 {
		dbOpts.WriteBuffer = lcfg.WriteBufferSize * opt.MiB
	}
	bloomFilterBits := lcfg.BloomFilterBits
	if bloomFilterBits <= 0 {
		bloomFilterBits = defaultBloomFilterBits
	}
	dbOpts.Filter = filter.NewBloomFilter(bloomFilterBits)
	//writeBatchSize := dbConfig.WriteBatchSize
	//if writeBatchSize <= 0 {
	//	writeBatchSize = defaultWriteBatchSize
	//}

	if lcfg.NoSync {
		dbOpts.NoSync = lcfg.NoSync
	}

	if lcfg.DisableBufferPool {
		dbOpts.DisableBufferPool = lcfg.DisableBufferPool
	}

	if lcfg.Compression > 0 {
		dbOpts.Compression = opt.Compression(lcfg.Compression)
	}

	if lcfg.DisableBlockCache {
		dbOpts.DisableBlockCache = lcfg.DisableBlockCache
	}

	if lcfg.BlockCacheCapacity > 0 {
		dbOpts.BlockCacheCapacity = lcfg.BlockCacheCapacity * opt.MiB
	}

	if lcfg.BlockSize > 0 {
		dbOpts.BlockSize = lcfg.BlockSize * opt.KiB
	}

	if lcfg.CompactionTableSize > 0 {
		dbOpts.CompactionTableSize = lcfg.CompactionTableSize * opt.MiB
	}

	if lcfg.CompactionTotalSize > 0 {
		dbOpts.CompactionTotalSize = lcfg.CompactionTotalSize * opt.MiB
	}

	if lcfg.WriteL0PauseTrigger > 0 {
		dbOpts.WriteL0PauseTrigger = lcfg.WriteL0PauseTrigger
	}

	if lcfg.WriteL0SlowdownTrigger > 0 {
		dbOpts.WriteL0SlowdownTrigger = lcfg.WriteL0SlowdownTrigger
	}

	if lcfg.CompactionL0Trigger > 0 {
		dbOpts.CompactionL0Trigger = lcfg.CompactionL0Trigger
	}

	//dbOpts.BlockCacheCapacity = 256 * opt.MiB
	//dbOpts.BlockSize = 128 * opt.KiB
	//dbOpts.CompactionL0Trigger = 16
	////dbOpts.CompactionSourceLimitFactor = 10
	//dbOpts.Compression = opt.NoCompression
	//dbOpts.CompactionTableSize = 512 * opt.MiB
	////dbOpts. = false
	//dbOpts.DisableBufferPool = false
	//dbOpts.DisableBlockCache = false
	//dbOpts.NoSync = true
	////dbOpts.DisableLargeBatchTransaction = false
	////dbOpts.WriteBuffer = 512 * opt.MiB
	//dbOpts.WriteL0PauseTrigger = 24
	//dbOpts.WriteL0SlowdownTrigger = 20

	return dbOpts
}

// createDirIfNotExist create dir if not exist
// @param string
// @return error
func createDirIfNotExist(path string) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		// 创建文件夹
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetDbType returns db type , "leveldb"(default)
// @return string
func (h *LevelDBHandle) GetDbType() string {
	return DbType_Leveldb
}

// Get returns the value for the given key, or returns nil if none exists
// @param []byte key
// @return []byte
// @return error
func (h *LevelDBHandle) Get(key []byte) ([]byte, error) {
	value, err := h.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		value = nil
		err = nil
	}
	if err != nil {
		h.logger.Errorf("getting leveldbprovider key [%s], err:%s", key, err.Error())
		return nil, errors.Wrapf(err, "error getting leveldbprovider key [%s]", key)
	}
	// if set encryptor, decrypt the value
	if h.encryptor != nil && len(value) > 0 {
		return h.encryptor.Decrypt(value)
	}
	return value, nil
}

// GetKeys returns the value for the given key
// @param [][]byte multi keys
// @return [][]byte multi values
// @return error
func (h *LevelDBHandle) GetKeys(keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	wg := sync.WaitGroup{}
	errsChan := make(chan error, len(keys))
	wg.Add(len(keys))
	values := make([][]byte, len(keys))
	// multi goroutines
	for index, k := range keys {
		go func(i int, key []byte) {
			defer wg.Done()

			if len(key) == 0 {
				values[i] = nil
				return
			}

			value, err := h.db.Get(key, nil)
			if err == leveldb.ErrNotFound {
				value = nil
				err = nil
			}
			values[i] = value
			if err != nil {
				h.logger.Errorf("getting leveldbprovider key [%s], err:%s", key, err.Error())
				errsChan <- errors.Wrapf(err, "error getting leveldbprovider key [%s]", key)
			}
			if h.encryptor != nil && len(value) > 0 {
				value, err = h.encryptor.Decrypt(value)
				if err != nil {
					errsChan <- err
				}
				values[i] = value
			}
		}(index, k)
	}

	wg.Wait()
	// got any mistake,return error
	if len(errsChan) > 0 {
		return nil, <-errsChan
	}

	return values, nil
}

// Put saves the key-values
// @param []byte key
// @param []byte value
// @return error
func (h *LevelDBHandle) Put(key []byte, value []byte) error {
	if value == nil {
		h.logger.Warn("writing leveldbprovider key [%s] with nil value", key)
		return errors.New("error writing leveldbprovider with nil value")
	}
	// if encryptor is not nil, encrypt the value
	if h.encryptor != nil && len(value) > 0 {
		var err error
		value, err = h.encryptor.Encrypt(value)
		if err != nil {
			return err
		}
	}
	// save key value in db(sync)
	err := h.db.Put(key, value, &opt.WriteOptions{Sync: true})
	if err != nil {
		h.logger.Errorf("writing leveldbprovider key [%s]", key)
		return errors.Wrapf(err, "error writing leveldbprovider key [%s]", key)
	}
	return err
}

// Has return true if the given key exist, or return false if none exists
// @param []byte key
// @return bool
// @return error
func (h *LevelDBHandle) Has(key []byte) (bool, error) {
	exist, err := h.db.Has(key, nil)
	if err != nil {
		h.logger.Errorf("getting leveldbprovider key [%s], err:%s", key, err.Error())
		return false, errors.Wrapf(err, "error getting leveldbprovider key [%s]", key)
	}
	return exist, nil
}

// Delete deletes the given key
// @param []byte key
// @return error
func (h *LevelDBHandle) Delete(key []byte) error {
	wo := &opt.WriteOptions{Sync: true}
	err := h.db.Delete(key, wo)
	if err != nil {
		h.logger.Errorf("deleting leveldbprovider key [%s]", key)
		return errors.Wrapf(err, "error deleting leveldbprovider key [%s]", key)
	}
	return err
}

// WriteBatch writes a batch in an atomic operation
// @param protocol.StoreBatcher
// @param bool sync
// @return error
func (h *LevelDBHandle) WriteBatch(batch protocol.StoreBatcher, sync bool) error {
	start := time.Now()
	if batch.Len() == 0 {
		return nil
	}
	//h.writeLock.Lock()
	//defer h.writeLock.Unlock()
	levelBatch, ok := h.batchPool.Get().(*leveldb.Batch)
	if !ok {
		h.logger.Errorf("get batch from batchPool err")
		return ErrBatch
	}
	levelBatch.Reset()

	//levelBatch := &leveldb.Batch{}
	for k, v := range batch.KVs() {
		key := []byte(k)
		if v == nil {
			levelBatch.Delete(key)
		} else {
			if h.encryptor != nil && len(v) > 0 {
				value, err := h.encryptor.Encrypt(v)
				if err != nil {
					return err
				}
				levelBatch.Put(key, value)
			} else {
				levelBatch.Put(key, v)
			}
		}
	}

	batchFilterDur := time.Since(start)
	wo := &opt.WriteOptions{Sync: sync}
	// batch write the save and delete operations
	h.writeLock.Lock()
	if err := h.db.Write(levelBatch, wo); err != nil {
		h.writeLock.Unlock()
		h.batchPool.Put(levelBatch)
		h.logger.Errorf("write batch to leveldb provider failed")
		return errors.Wrap(err, "error writing batch to leveldb provider")
	}
	h.writeLock.Unlock()
	h.batchPool.Put(levelBatch)
	writeDur := time.Since(start)
	h.logger.Debugf("leveldb write batch[%d] sync: %v, time used: (filter:%d, write:%d, total:%d)",
		batch.Len(), sync, batchFilterDur.Milliseconds(), (writeDur - batchFilterDur).Milliseconds(),
		time.Since(start).Milliseconds())
	return nil
}

// CompactRange compacts the underlying DB for the given key range.
// @param []byte start
// @param []byte limit
// @return error
func (h *LevelDBHandle) CompactRange(start, limit []byte) error {
	return h.db.CompactRange(util.Range{
		Start: start,
		Limit: limit,
	})
}

// NewIteratorWithRange returns an iterator that contains all the key-values between given key ranges
// start is included in the results and limit is excluded.
// @param []byte startKey
// @param []byte limitKey
// @return protocol.Iterator
// @return error
func (h *LevelDBHandle) NewIteratorWithRange(startKey []byte, limitKey []byte) (protocol.Iterator, error) {
	if len(startKey) == 0 || len(limitKey) == 0 {
		return nil, fmt.Errorf("iterator range should not start(%s) or limit(%s) with empty key",
			string(startKey), string(limitKey))
	}
	keyRange := &util.Range{Start: startKey, Limit: limitKey}
	// construct an iterator
	iter := h.db.NewIterator(keyRange, nil)
	if h.encryptor != nil {
		return NewEncryptedIterator(iter, h.encryptor), nil
	}
	return iter, nil
}

// NewIteratorWithPrefix returns an iterator that contains all the key-values with given prefix
// @param []byte prefix
// @return protocol.Iterator
// @return error
func (h *LevelDBHandle) NewIteratorWithPrefix(prefix []byte) (protocol.Iterator, error) {
	if len(prefix) == 0 {
		return nil, fmt.Errorf("iterator prefix should not be empty key")
	}
	r := util.BytesPrefix(prefix)
	return h.NewIteratorWithRange(r.Start, r.Limit)
}

// Close closes the leveldb
// @return error
func (h *LevelDBHandle) Close() error {
	h.writeLock.Lock()
	defer h.writeLock.Unlock()
	return h.db.Close()
}

// EncryptedIterator 具有数据解密的迭代器
type EncryptedIterator struct {
	it        iterator.Iterator
	encryptor crypto.SymmetricKey
	err       error
}

// NewEncryptedIterator construct an Encrypted Iterator
// @param iterator.Iterator
// @param crypto.SymmetricKey
// @return *EncryptedIterator
func NewEncryptedIterator(it iterator.Iterator, encryptor crypto.SymmetricKey) *EncryptedIterator {
	return &EncryptedIterator{it: it, encryptor: encryptor}
}

// Next check iterator whether has next element
// @return bool
func (e *EncryptedIterator) Next() bool {
	return e.it.Next()
}

// First reset the iterator
// @return bool
func (e *EncryptedIterator) First() bool {
	return e.it.First()
}

// Error return any error generated by EncryptedIterator
// @return error
func (e *EncryptedIterator) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.it.Error()
}

// Key reutrn current key
// @return []byte key
func (e *EncryptedIterator) Key() []byte {
	return e.it.Key()
}

// Value return current value, and decrypt the value
// @return []byte value located at currentIndex
func (e *EncryptedIterator) Value() []byte {
	value := e.it.Value()
	if len(value) > 0 {
		v, err := e.encryptor.Decrypt(value)
		if err != nil {
			e.err = err
		}
		return v
	}
	return value
}

// Release close the db ,release the iterator
func (e *EncryptedIterator) Release() {
	e.it.Release()
}

// newBatch returns a leveldb.Batch
func newBatch() interface{} {
	return &leveldb.Batch{}
}
