/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package tikvdbprovider

import (
	"context"
	"errors"

	"github.com/tikv/client-go/rawkv"
)

// Iterator will walk through the key between startKey and endKey and return paired value.
// since protocol.ParametersValueMaxLength == 1024 * 1024 byte == 1MB, so we can just fetch a batch of
// values in memory without worrying about too much memory occupied, default batchSize is 10 (can modify in need),
// and we will keep this batch of in use kvs in memory, and fresh it if iter entered next or previous batch
type Iterator struct {
	db        *rawkv.Client
	keyPrefix []byte
	totalKeys [][]byte
	limit     int

	startKey  []byte
	endKey    []byte
	bacthKeys [][]byte
	bacthVals [][]byte
	curIndex  int
	batchSize int
	first     bool

	ctx context.Context
}

// NewIterator construct an iterator
// @param *rawkv.Client dbclient
// @param []byte start
// @param []byte end
// @param int limit
// @param int batchSize
// @param []byte keyPrefix
// @return *Iterator
// @return error
func NewIterator(db *rawkv.Client, start []byte, end []byte, limit int,
	batchSize int, keyPrefix []byte) (*Iterator, error) {
	if len(start) == 0 || len(end) == 0 || limit <= 0 || batchSize > limit {
		return nil, errors.New("invalid tikv iter init param")
	}

	iter := &Iterator{
		db:        db,
		keyPrefix: keyPrefix,
		limit:     limit,
		startKey:  start,
		endKey:    end,
		batchSize: batchSize,
		curIndex:  0,
		first:     true,
		ctx:       context.Background(),
	}

	option := rawkv.DefaultScanOption()
	option.KeyOnly = true
	// scan keys from start to end ,at the most limit
	totalKeys, _, err := db.Scan(iter.ctx, start, end, limit, option)
	if err != nil {
		return nil, err
	}
	if len(totalKeys) == 0 || len(totalKeys[0]) == 0 {
		return iter, nil
	}

	option.KeyOnly = false
	// scan key , value from start to end
	keys, vals, err1 := db.Scan(iter.ctx, start, end, batchSize, option)
	if err1 != nil {
		return nil, err1
	}

	iter.totalKeys = totalKeys
	iter.bacthKeys = keys
	iter.bacthVals = vals
	return iter, nil
}

// Key reutrn current key
// @return []byte key
func (iter *Iterator) Key() []byte {
	if len(iter.totalKeys) <= iter.curIndex {
		return nil
	}

	key := iter.totalKeys[iter.curIndex]
	if len(key) < len(iter.keyPrefix) {
		return key
	}
	return key[len(iter.keyPrefix):]
}

// Value return current value
// @return []byte value located at currentIndex
func (iter *Iterator) Value() []byte {
	bIndex := iter.curIndex % iter.batchSize
	if len(iter.bacthVals) > bIndex {
		return iter.bacthVals[bIndex]
	}
	return nil
}

// Next check iterator whether has next element
// @return bool
func (iter *Iterator) Next() bool {
	//if len(iter.totalKeys) == 0 || len(iter.totalKeys) == iter.curIndex+1 {
	if len(iter.totalKeys) == 0 {
		return false
	}

	if iter.first {
		iter.first = false
		return true
	}
	if len(iter.totalKeys) == iter.curIndex+1 {
		return false
	}

	//curIndex enter next  batch kvs, will fetch next batch kvs to memory
	if iter.curIndex%iter.batchSize == 0 {
		option := rawkv.DefaultScanOption()
		option.KeyOnly = false
		//startKey := makePrefixedKey(iter.bacthKeys[iter.curIndex%iter.batchSize], iter.keyPrefix)
		keys, vals, err := iter.db.Scan(iter.ctx,
			iter.bacthKeys[iter.curIndex%iter.batchSize], iter.endKey, iter.batchSize, option)
		if err != nil || len(keys) == 0 {
			return false
		}
		iter.bacthKeys = keys
		iter.bacthVals = vals
		iter.curIndex++
		return true
	}

	iter.curIndex++
	return len(iter.bacthKeys)-(iter.curIndex%iter.batchSize) > 0
}

// First reset the iterator
// @return bool
func (iter *Iterator) First() bool {
	option := rawkv.DefaultScanOption()
	option.KeyOnly = false
	// scan data from db
	keys, vals, err := iter.db.Scan(iter.ctx, iter.startKey, iter.endKey, iter.batchSize, option)
	if err != nil {
		return false
	}

	iter.curIndex = 0
	iter.bacthKeys = keys
	iter.bacthVals = vals
	return true
}

// Error return nil
// @return error (default nil)
func (iter *Iterator) Error() error {
	return nil
}

// Release close the db ,release the iterator
func (iter *Iterator) Release() {
	iter.ctx.Done()
	//the tikv client is a singleton of the handler and cannot be closed
	// iter.db.Close()
}
