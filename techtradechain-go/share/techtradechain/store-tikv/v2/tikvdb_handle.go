/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package tikvdbprovider

import (
	"context"
	"fmt"
	"strings"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/pkg/errors"
	tikvcfg "github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

var _ protocol.DBHandle = (*TiKVDBHandle)(nil)

const (
	defaultEndpionts            = "127.0.0.1:2379"
	defaultMaxBatchCnt          = 128
	defaultGrpcConnectionCount  = 4
	defaultGrpcKeepAliveTime    = 10
	defaultGrpcKeepAliveTimeout = 3
	defaultWriteBatchSize       = 128
	defaultMaxScanLimit         = 10240
	defaultScanBatchSize        = 10
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
	//PrefixSeparator prefix and raw key separator
	PrefixSeparator = "#"
)

// TiKVDBHandle encapsulated handle to tikvdb
type TiKVDBHandle struct {
	db             *rawkv.Client
	logger         protocol.Logger
	addrs          []string
	dbprefix       []byte
	writeBatchSize uint64
	maxScanLimit   uint64
	scanBatchSize  uint64
	ctx            context.Context
}

// NewTikvDBOptions used to build TiKVDBHandle
type NewTikvDBOptions struct {
	Config    *TiKVDbConfig
	Logger    protocol.Logger
	Encryptor crypto.SymmetricKey
	ChainId   string
	DbName    string
}

// NewTiKVDBHandle use NewTikvDBOptions to build TiKVDBHandle
// @param NewTikvDBOptions config
// @return *TiKVDBHandle dbhandler
func NewTiKVDBHandle(input *NewTikvDBOptions) *TiKVDBHandle {
	dbconfig := input.Config
	if len(dbconfig.DbPrefix) == 0 {
		dbconfig.DbPrefix = ""
	}
	dbprefix := fmt.Sprintf("%s%s_%s%s", dbconfig.DbPrefix, input.ChainId, input.DbName, PrefixSeparator)
	tdh := &TiKVDBHandle{
		logger:         input.Logger,
		dbprefix:       []byte(dbprefix),
		writeBatchSize: defaultWriteBatchSize,
		ctx:            context.Background(),
	}

	if len(dbconfig.Endpoints) > 0 {
		endpoints := strings.Split(dbconfig.Endpoints, ",")
		tdh.addrs = append(tdh.addrs, endpoints...)
		if len(tdh.addrs) == 0 {
			tdh.logger.Warnf("can not parse any validate format endpoint, will use default endpoint: %s", defaultEndpionts)
			tdh.addrs = append(tdh.addrs, defaultEndpionts)
		}
	} else {
		tdh.logger.Info("use will use default endpoint: %s", defaultEndpionts)
		tdh.addrs = append(tdh.addrs, defaultEndpionts)
	}

	cfg := tikvcfg.Default()
	cfg.RPC.Batch.MaxBatchSize = defaultMaxBatchCnt
	cfg.RPC.MaxConnectionCount = defaultGrpcConnectionCount
	cfg.RPC.GrpcKeepAliveTime = defaultGrpcKeepAliveTime
	cfg.RPC.GrpcKeepAliveTimeout = defaultGrpcKeepAliveTimeout

	tdh.maxScanLimit = defaultMaxScanLimit
	tdh.scanBatchSize = defaultScanBatchSize
	// config tikv client
	if dbconfig.MaxBatchCount > 0 {
		cfg.RPC.Batch.MaxBatchSize = dbconfig.MaxBatchCount
	}
	if dbconfig.GrpcConnectionCount > 0 {
		cfg.RPC.MaxConnectionCount = dbconfig.GrpcConnectionCount
	}
	if dbconfig.GrpcKeepAliveTime > 0 {
		cfg.RPC.GrpcKeepAliveTime = time.Duration(dbconfig.GrpcKeepAliveTime) * time.Second
	}
	if dbconfig.GrpcKeepAliveTimeout > 0 {
		cfg.RPC.GrpcKeepAliveTimeout = time.Duration(dbconfig.GrpcKeepAliveTimeout) * time.Second
	}

	if dbconfig.WriteBatchSize > 0 {
		tdh.writeBatchSize = dbconfig.WriteBatchSize
	}

	if dbconfig.MaxScanLimit > 0 {
		tdh.maxScanLimit = dbconfig.MaxScanLimit
	}

	if dbconfig.ScanBatchSize > 0 {
		tdh.scanBatchSize = dbconfig.ScanBatchSize
	}

	// creates a client with PD cluster addrs
	cli, err := rawkv.NewClient(tdh.ctx, tdh.addrs, cfg)
	if err != nil {
		panic(fmt.Sprintf("Error opening %s by tikvdbprovider: %s", tdh.addrs[0], err))
	}
	tdh.logger.Debugf("open tikv: %s, and cluser id: %d", tdh.addrs[0], cli.ClusterID())
	tdh.db = cli
	return tdh
}

//GetDbType returns db type
// @ return string
func (h *TiKVDBHandle) GetDbType() string {
	return "tikv"
}

// Get returns the value for the given key, or returns nil if none exists
// @param []byte key
// @return []byte value
// @return error
func (h *TiKVDBHandle) Get(key []byte) ([]byte, error) {
	fkey := makePrefixedKey(key, h.dbprefix)
	// get value
	value, err := h.db.Get(h.ctx, makePrefixedKey(key, h.dbprefix))
	if err != nil {
		h.logger.Errorf("getting tikvdbprovider key [%s], err:%s", fkey, err.Error())
		return nil, errors.Wrapf(err, "error getting tikvdbprovider key [%s]", fkey)
	}
	return value, nil
}

// GetKeys returns the value for the given key
func (h *TiKVDBHandle) GetKeys(keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	fKeys := make([][]byte, len(keys))
	for i, k := range keys {
		fKeys[i] = makePrefixedKey(k, h.dbprefix)
	}
	values, err := h.db.BatchGet(h.ctx, fKeys)
	if err != nil {
		h.logger.Errorf("getting tikvdbprovider [%d] keys, first key [%s], err:%s", len(fKeys), fKeys[0], err.Error())
		return nil, errors.Wrapf(err, "error getting tikvdbprovider [%d] keys", len(fKeys))
	}

	return values, nil
}

// Put saves the key-values
// @param []byte key
// @param []byte value
// @return error
func (h *TiKVDBHandle) Put(key []byte, value []byte) error {
	fkey := makePrefixedKey(key, h.dbprefix)
	if value == nil {
		h.logger.Warnf("writing tikvdbprovider key [%s] with nil value", fkey)
		return errors.New("error writing tikvdbprovider with nil value")
	}
	// put key value in db
	err := h.db.Put(h.ctx, fkey, value)
	if err != nil {
		h.logger.Errorf("writing tikvdbprovider key [%s]", fkey)
		return errors.Wrapf(err, "error writing tikvdbprovider key [%s]", fkey)
	}
	return err
}

// Has return true if the given key exist, or return false if none exists
// @param []byte key
// @return bool ,whether key exists
// @return error
func (h *TiKVDBHandle) Has(key []byte) (bool, error) {
	fkey := makePrefixedKey(key, h.dbprefix)
	// get value by key
	value, err := h.db.Get(h.ctx, fkey)
	if err != nil {
		h.logger.Errorf("getting tikvdbprovider key [%s], err:%s", fkey, err.Error())
		return false, errors.Wrapf(err, "error getting tikvdbprovider key [%s]", fkey)
	}
	// value not nil ,return true
	return len(value) > 0, nil
}

// Delete deletes the given key
// @param []byte key
// @return error
func (h *TiKVDBHandle) Delete(key []byte) error {
	fkey := makePrefixedKey(key, h.dbprefix)
	// delete the key / value
	err := h.db.Delete(h.ctx, fkey)
	if err != nil {
		h.logger.Errorf("deleting tikvdbprovider key [%s]", fkey)
		return errors.Wrapf(err, "error deleting tikvdbprovider key [%s]", fkey)
	}
	return err
}

// WriteBatch writes a batch in an atomic operation
// @param protocol.StoreBatcher
// @param bool
// @return error
func (h *TiKVDBHandle) WriteBatch(batch protocol.StoreBatcher, _ bool) error {
	start := time.Now()
	if batch.Len() == 0 {
		return nil
	}
	delKeys := make([][]byte, 0, 10)
	keys := make([][]byte, 0, batch.Len())
	values := make([][]byte, 0, batch.Len())
	// fulfill delkey and put-key-value
	for k, v := range batch.KVs() {
		key := []byte(k)
		if v == nil {
			delKeys = append(delKeys, makePrefixedKey(key, h.dbprefix))
		} else {
			keys = append(keys, makePrefixedKey(key, h.dbprefix))
			values = append(values, v)
		}
	}

	batchFilterDur := time.Since(start)
	// batch delete the key
	if err := h.db.BatchDelete(h.ctx, delKeys); err != nil {
		h.logger.Errorf("write batch delete keys to tikvdbprovider failed")
		return errors.Wrap(err, "error writing batch to tikvdbprovider")
	}
	// batch put key value
	if err := h.db.BatchPut(h.ctx, keys, values); err != nil { //
		h.logger.Errorf("write batch to tikvdbprovider failed")
		return errors.Wrap(err, "error writing batch to tikvdbprovider")
	}
	writeDur := time.Since(start)
	h.logger.Debugf("tikvdb write batch[%d] sync: none, time used: (filter:%d, write:%d, total:%d)",
		batch.Len(), batchFilterDur.Milliseconds(), (writeDur - batchFilterDur).Milliseconds(),
		time.Since(start).Milliseconds())
	return nil
}

// CompactRange compacts the underlying DB for the given key range.
// @param []byte
// @param []byte
// @return error
func (h *TiKVDBHandle) CompactRange(_, _ []byte) error {
	h.logger.Infof("TiKV skip compact range")
	return nil
}

// NewIteratorWithRange returns an iterator that contains all the key-values between given key ranges
// start is included in the results and limit is excluded.
// @param []byte startKey
// @param []byte limitKey
// @return protocol.Iterator
// @return error
func (h *TiKVDBHandle) NewIteratorWithRange(startKey []byte, limitKey []byte) (protocol.Iterator, error) {
	if len(startKey) == 0 || len(limitKey) == 0 {
		return nil, fmt.Errorf("iterator range should not start(%s) or limit(%s) with empty key",
			string(startKey), string(limitKey))
	}

	return NewIterator(h.db, makePrefixedKey(startKey, h.dbprefix),
		makePrefixedKey(limitKey, h.dbprefix), int(h.maxScanLimit), int(h.scanBatchSize), h.dbprefix)
}

// NewIteratorWithPrefix returns an iterator that contains all the key-values with given prefix
// @param []byte prefix
// @return protocol.Iterator
// @return error
func (h *TiKVDBHandle) NewIteratorWithPrefix(prefix []byte) (protocol.Iterator, error) {
	return h.NewIteratorWithRange(bytesPrefix(prefix))
}

// GetWriteBatchSize return writeBatchSize
// @return uint64
func (h *TiKVDBHandle) GetWriteBatchSize() uint64 {
	return h.writeBatchSize
}

// Close closes the tikvdb
func (h *TiKVDBHandle) Close() error {
	h.ctx.Done()
	return h.db.Close()
}
