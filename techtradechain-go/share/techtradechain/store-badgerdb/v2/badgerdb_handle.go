/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package badgerdbprovider

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/pkg/errors"
)

// check BadgerDBHandle implements protocol.DBHandle interface
var _ protocol.DBHandle = (*BadgerDBHandle)(nil)

const (
	// defaultCompression denote compression
	defaultCompression = 0
	// defaultValueThreshold default threshold
	defaultValueThreshold = 1024 * 10
	// defaultWriteBatchSize default write batch size
	defaultWriteBatchSize = 128
)

// BadgerDBHandle encapsulated handle to badgerDB
type BadgerDBHandle struct {
	writeLock      sync.Mutex
	db             *badger.DB
	logger         protocol.Logger
	writeBatchSize uint64
}

// NewBadgerDBOptions used to build  BadgerDBHandle
type NewBadgerDBOptions struct {
	Config    *BadgerDbConfig
	Logger    protocol.Logger
	Encryptor crypto.SymmetricKey
	ChainId   string
	DbFolder  string
}

// NewBadgerDBHandle use NewBadgerDBOptions to build BadgerDBHandle
// @param *NewBadgerDBOptions config option
// @return *BadgerDBHandle
func NewBadgerDBHandle(input *NewBadgerDBOptions) *BadgerDBHandle {
	chainId := input.ChainId
	dbFolder := input.DbFolder
	dbconfig := input.Config
	logger := input.Logger
	// make db store path
	dbPath := filepath.Join(dbconfig.StorePath, chainId, dbFolder)
	opt := badger.DefaultOptions(dbPath)
	opt.SyncWrites = false
	opt.Compression = defaultCompression
	opt.ValueThreshold = defaultValueThreshold

	if dbconfig.Compression != 0 && dbconfig.Compression < 3 {
		opt.Compression = options.CompressionType(dbconfig.Compression)
	}
	if dbconfig.ValueThreshold > 0 {
		opt.ValueThreshold = dbconfig.ValueThreshold
	}
	writeBatchSize := uint64(defaultWriteBatchSize)
	if dbconfig.WriteBatchSize > 0 {
		writeBatchSize = dbconfig.WriteBatchSize
	}
	// create store path
	err := createDirIfNotExist(dbPath)
	if err != nil {
		panic(fmt.Sprintf("Error create dir %s by badgerdbprovider: %s", dbPath, err))
	}
	// create db use the given path
	db, err := badger.Open(opt)
	if err != nil {
		panic(fmt.Sprintf("Error opening %s by badgerdbprovider: %s", dbPath, err))
	}
	logger.Debugf("open badgerdb:%s", dbPath)
	return &BadgerDBHandle{
		db:             db,
		logger:         logger,
		writeBatchSize: writeBatchSize,
	}
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
func (h *BadgerDBHandle) GetDbType() string {
	return "badgerdb"
}

// Get returns the value for the given key, or returns nil if none exists
// @param []byte key
// @return []byte
// @return error
func (h *BadgerDBHandle) Get(key []byte) ([]byte, error) {
	var value []byte
	err := h.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})

	if err == badger.ErrKeyNotFound {
		value = nil
		err = nil
	} else if err != nil {
		h.logger.Errorf("getting badgerdbprovider key [%s], err:%s", key, err.Error())
		return nil, errors.Wrapf(err, "error getting badgerdbprovider key [%s]", key)
	}
	return value, nil
}

// GetKeys returns the value for the given key
// @param [][]byte multi keys
// @return [][]byte multi values
// @return error
func (h *BadgerDBHandle) GetKeys(keys [][]byte) ([][]byte, error) {
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

			var value []byte
			err := h.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				value, err = item.ValueCopy(nil)
				return err
			})

			if err == badger.ErrKeyNotFound {
				value = nil
				err = nil
			}

			if err != nil {
				h.logger.Errorf("getting badgerdbprovider key [%s], err:%s", key, err.Error())
				errsChan <- errors.Wrapf(err, "error getting badgerdbprovider key [%s]", key)
			}
			values[i] = value
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
func (h *BadgerDBHandle) Put(key []byte, value []byte) error {
	if value == nil {
		h.logger.Warn("writing badgerdbprovider key [%s] with nil value", key)
		return errors.New("error writing badgerdbprovider with nil value")
	}
	wb := h.db.NewWriteBatch()
	err := wb.Set(key, value)
	if err != nil {
		return err
	}
	err = wb.Flush()
	if err != nil {
		h.logger.Errorf("writing badgerdbprovider key [%s]", key)
		return errors.Wrapf(err, "error writing badgerdbprovider key [%s]", key)
	}
	return err
}

// Has return true if the given key exist, or return false if none exists
// @param []byte key
// @return bool
// @return error
func (h *BadgerDBHandle) Has(key []byte) (bool, error) {
	exist := false
	err := h.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			return err
		}
		exist = true
		return nil
	})

	if err == badger.ErrKeyNotFound {
		exist = false
		err = nil
	} else if err != nil {
		h.logger.Errorf("getting badgerdbprovider key [%s], err:%s", key, err.Error())
		return false, errors.Wrapf(err, "error getting badgerdbprovider key [%s]", key)
	}
	return exist, nil
}

// Delete deletes the given key
// @param []byte key
// @return error
func (h *BadgerDBHandle) Delete(key []byte) error {
	wb := h.db.NewWriteBatch()
	defer wb.Cancel()
	err := wb.Delete(key)
	if err != nil {
		h.logger.Errorf("deleting badgerdbprovider key [%s]", key)
		return errors.Wrapf(err, "error deleting badgerdbprovider key [%s]", key)
	}
	return err
}

// WriteBatch writes a batch in an atomic operation
//nolint:golint,unused
// @param protocol.StoreBatcher
// @param bool sync
// @return error
func (h *BadgerDBHandle) WriteBatch(batch protocol.StoreBatcher, sync bool) error {
	start := time.Now()
	if batch.Len() == 0 {
		return nil
	}
	h.writeLock.Lock()
	defer h.writeLock.Unlock()
	badgerBatch := h.db.NewWriteBatch()
	for k, v := range batch.KVs() {
		key := []byte(k)
		if v == nil {
			_ = badgerBatch.Delete(key)
		} else {
			_ = badgerBatch.Set(key, v)
		}
	}

	batchFilterDur := time.Since(start)
	if err := badgerBatch.Flush(); err != nil {
		h.logger.Errorf("write batch to badgerdb provider failed")
		return errors.Wrap(err, "error writing batch to badgerdb provider")
	}
	writeDur := time.Since(start)
	h.logger.Debugf("badgerdb write batch[%d] sync: none, time used: (filter:%d, write:%d, total:%d)",
		batch.Len(), batchFilterDur.Milliseconds(), (writeDur - batchFilterDur).Milliseconds(),
		time.Since(start).Milliseconds())
	return nil
}

// CompactRange compacts the underlying DB for the given key range.
// @param []byte start
// @param []byte limit
// @return error (default nil)
func (h *BadgerDBHandle) CompactRange(start, limit []byte) error { //nolint:golint,unused
	return nil
	//return h.db.CompactRange(util.Range{
	//	Start: start,
	//	Limit: limit,
	//})
}

// NewIteratorWithRange returns an iterator that contains all the key-values between given key ranges
// start is included in the results and limit is excluded.
// @param []byte startKey
// @param []byte limitKey
// @return protocol.Iterator
// @return error
func (h *BadgerDBHandle) NewIteratorWithRange(startKey []byte, limitKey []byte) (protocol.Iterator, error) {
	if len(startKey) == 0 || len(limitKey) == 0 {
		return nil, fmt.Errorf("iterator range should not start(%s) or limit(%s) with empty key",
			string(startKey), string(limitKey))
	}
	return NewIterator(h.db, badger.DefaultIteratorOptions, startKey, limitKey), nil
}

// NewIteratorWithPrefix returns an iterator that contains all the key-values with given prefix
// @param []byte prefix
// @return protocol.Iterator
// @return error
func (h *BadgerDBHandle) NewIteratorWithPrefix(prefix []byte) (protocol.Iterator, error) {
	if len(prefix) == 0 {
		return nil, fmt.Errorf("iterator prefix should not be empty key")
	}

	return h.NewIteratorWithRange(bytesPrefix(prefix))
}

// GetWriteBatchSize returns write batch size
// @return uint64
func (h *BadgerDBHandle) GetWriteBatchSize() uint64 {
	return h.writeBatchSize
}

// Close closes the badgerdb
// @return error
func (h *BadgerDBHandle) Close() error {
	h.writeLock.Lock()
	defer h.writeLock.Unlock()
	return h.db.Close()
}

// bytesPrefix returns key range that satisfy the given prefix.
// This only applicable for the standard 'bytes comparer'.
// @param []byte prefix
// @return []byte start
// @return []byte end
func bytesPrefix(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}
