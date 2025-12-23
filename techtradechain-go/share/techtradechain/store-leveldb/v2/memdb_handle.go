/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package leveldbprovider

import (
	"bytes"
	"fmt"
	"sync"

	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var _ protocol.DBHandle = (*MemdbHandle)(nil)

// MemdbHandle in-memory key/value database handler
type MemdbHandle struct {
	db     *memdb.DB
	closed bool
}

var errClosed = errors.New("leveldb: closed")

type bytesComparer struct{}

// Compare returns an integer comparing two byte slices lexicographically.
// @param []byte a
// @param []byte b
// @return int  0 if a==b, -1 if a < b, and +1 if a > b
func (bytesComparer) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// NewMemdbHandle construct default MemdbHandle
// @return *MemdbHandle
func NewMemdbHandle() *MemdbHandle {
	return &MemdbHandle{db: memdb.New(&bytesComparer{}, 1000)}
}

// GetDbType return "leveldb"
// @return string
func (db *MemdbHandle) GetDbType() string {
	return DbType_Leveldb
}

// Get get value
// @param []byte key
// @return []byte value
// @return error
func (db *MemdbHandle) Get(key []byte) ([]byte, error) {
	if db.closed {
		return nil, errClosed
	}
	return db.db.Get(key)
}

// GetKeys use multi goroutine to get keys
// @param [][]byte multi keys
// @return [][]byte multi values
// @return error
func (db *MemdbHandle) GetKeys(keys [][]byte) ([][]byte, error) {
	if db.closed {
		return nil, errClosed
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

			value, err := db.Get(key)
			if err == memdb.ErrNotFound {
				value = nil
				err = nil
			}
			values[i] = value
			if err != nil {
				errsChan <- errors.Wrapf(err, "error getting memdbprovider key [%s]", key)
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
func (db *MemdbHandle) Put(key []byte, value []byte) error {
	if db.closed {
		return errClosed
	}
	return db.db.Put(key, value)
}

// Has return true if the given key exist, or return false if none exists
// @param []byte key
// @return bool
// @return error
func (db *MemdbHandle) Has(key []byte) (bool, error) {
	if db.closed {
		return false, errClosed
	}
	return db.db.Contains(key), nil
}

// Delete deletes the given key
// @param []byte key
// @return error
func (db *MemdbHandle) Delete(key []byte) error {
	if db.closed {
		return errClosed
	}
	return db.db.Delete(key)
}

// WriteBatch writes a batch in an atomic operation
// @param protocol.StoreBatcher
// @param bool sync
// @return error
func (db *MemdbHandle) WriteBatch(batch protocol.StoreBatcher, sync bool) error {
	if db.closed {
		return errClosed
	}
	for k, v := range batch.KVs() {
		if err := db.db.Put([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

// NewIteratorWithRange returns an iterator that contains all the key-values between given key ranges
// start is included in the results and limit is excluded.
// @param []byte start
// @param []byte limit
// @return protocol.Iterator
// @return error
func (db *MemdbHandle) NewIteratorWithRange(start []byte, limit []byte) (protocol.Iterator, error) {
	if len(start) == 0 || len(limit) == 0 {
		return nil, fmt.Errorf("iterator range should not start(%s) or limit(%s) with empty key",
			string(start), string(limit))
	}
	if db.closed {
		return nil, errClosed
	}
	return db.db.NewIterator(&util.Range{Start: start, Limit: limit}), nil
}

// NewIteratorWithPrefix returns an iterator that contains all the key-values with given prefix
// @param []byte prefix
// @return protocol.Iterator
// @return error
func (db *MemdbHandle) NewIteratorWithPrefix(prefix []byte) (protocol.Iterator, error) {
	if len(prefix) == 0 {
		return nil, fmt.Errorf("iterator prefix should not be empty key")
	}
	if db.closed {
		return nil, errClosed
	}
	return db.db.NewIterator(util.BytesPrefix(prefix)), nil
}

// GetWriteBatchSize return writeBatchSize , 0 (default)
// @return uint64
func (db *MemdbHandle) GetWriteBatchSize() uint64 {
	return 0
}

// CompactRange return nil (default)
// @param []byte start
// @param []byte limit
// @return error
func (db *MemdbHandle) CompactRange(start []byte, limit []byte) error {
	return nil
}

// Close reset db to initial empty state
// @return error
func (db *MemdbHandle) Close() error {
	if db.closed {
		return errClosed
	}
	db.db.Reset()
	db.closed = true
	return nil
}
