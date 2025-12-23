/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package badgerdbprovider

import (
	"bytes"

	"github.com/dgraph-io/badger/v3"
)

// Iterator encapsulated badger-db iterator
type Iterator struct {
	db    *badger.DB
	biter *badger.Iterator
	opts  *badger.IteratorOptions
	txn   *badger.Txn
	start []byte
	end   []byte
	first bool
}

// NewIterator  construct Iterator
// @param *badger.DB db instance
// @param badger.IteratorOptions opt
// @param []byte start
// @param []byte end
// @return *Iterator
func NewIterator(db *badger.DB, opts badger.IteratorOptions, start []byte, end []byte) *Iterator {
	txn := db.NewTransaction(false)
	iter := &Iterator{
		db:    db,
		biter: txn.NewIterator(opts),
		start: start,
		end:   end,
		txn:   txn,
		opts:  &opts,
		first: true,
	}

	if iter.isRangeIter() {
		iter.biter.Seek(iter.start)
	} else {
		iter.biter.Rewind()
	}

	return iter
}

// Key reutrn current key
// @return []byte key
func (iter *Iterator) Key() []byte {
	if iter.biter.Valid() {
		return iter.biter.Item().Key()
	}
	return nil
}

// Value return current value
// @return []byte value located at currentIndex
func (iter *Iterator) Value() []byte {
	if iter.biter.Valid() {
		item := iter.biter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil
		}
		return value
	}

	return nil
}

// Next check iterator whether has next element
// @return bool
func (iter *Iterator) Next() bool {
	if !iter.isValid() {
		return false
	}

	if iter.first {
		iter.first = false
	} else {
		iter.biter.Next()
	}

	if iter.isRangeIter() {
		// if start and end are both not nil, this is a range iter
		if iter.biter.Valid() {
			item := iter.biter.Item()
			return bytes.Compare(item.Key(), iter.start) >= 0 && bytes.Compare(item.Key(), iter.end) < 0
		}
		return false
	}
	//else should be prefix iter
	return iter.isValid()
}

// First reset the iterator
// @return bool
func (iter *Iterator) First() bool {
	if iter.isRangeIter() {
		iter.biter.Seek(iter.start)
	} else {
		iter.biter.Rewind()
	}

	return iter.isValid()
}

// Error return error ,nil(default)
// @return error
func (iter *Iterator) Error() error {
	return nil
}

// Release close the db ,release the iterator
func (iter *Iterator) Release() {
	iter.biter.Close()
	iter.txn.Discard()
}

// isValid returns false when iteration is done
// @return bool
func (iter *Iterator) isValid() bool {
	if iter.biter.Valid() {
		if len(iter.opts.Prefix) != 0 {
			return iter.biter.ValidForPrefix(iter.opts.Prefix)
		}
		return true
	}

	return false
}

// isRangeIter return true when start and end are both not nill
// @return bool
func (iter *Iterator) isRangeIter() bool {
	return len(iter.start) != 0 && len(iter.end) != 0
}
