/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package store

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/pkcs11"
	"techtradechain.com/techtradechain/common/v2/wal"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/store/v2/bigfilterdb"
	"techtradechain.com/techtradechain/store/v2/binlog"
	"techtradechain.com/techtradechain/store/v2/blockdb"
	"techtradechain.com/techtradechain/store/v2/blockdb/blockfiledb"
	"techtradechain.com/techtradechain/store/v2/blockdb/blockkvdb"
	"techtradechain.com/techtradechain/store/v2/blockdb/blocksqldb"
	"techtradechain.com/techtradechain/store/v2/cache"
	"techtradechain.com/techtradechain/store/v2/conf"
	"techtradechain.com/techtradechain/store/v2/contracteventdb"
	"techtradechain.com/techtradechain/store/v2/contracteventdb/eventsqldb"
	"techtradechain.com/techtradechain/store/v2/dbprovider"
	"techtradechain.com/techtradechain/store/v2/historydb"
	"techtradechain.com/techtradechain/store/v2/historydb/historykvdb"
	"techtradechain.com/techtradechain/store/v2/historydb/historysqldb"
	"techtradechain.com/techtradechain/store/v2/resultdb"
	"techtradechain.com/techtradechain/store/v2/resultdb/resultfiledb"
	"techtradechain.com/techtradechain/store/v2/resultdb/resultkvdb"
	"techtradechain.com/techtradechain/store/v2/resultdb/resultsqldb"
	"techtradechain.com/techtradechain/store/v2/rolling_window_cache"
	"techtradechain.com/techtradechain/store/v2/statedb"
	"techtradechain.com/techtradechain/store/v2/statedb/statekvdb"
	"techtradechain.com/techtradechain/store/v2/statedb/statesqldb"
	"techtradechain.com/techtradechain/store/v2/test"
	"techtradechain.com/techtradechain/store/v2/txexistdb"
	"techtradechain.com/techtradechain/store/v2/txexistdb/txexistkvdb"
)

type (
	instanceCreator func(out interface{}) error
	localDB         = protocol.DBHandle
)

type storeBuilder struct {
	chainId         string
	config          *conf.StorageConfig
	logger          protocol.Logger
	p11Handle       *pkcs11.P11Handle
	dbFactory       *dbprovider.DBFactory
	instanceCreator map[reflect.Type]instanceCreator
	//need construct items
	symmetricKey crypto.SymmetricKey
	//binLogger(walLog) is singleton shared by other components for the store instance.
	binLogger       binlog.BinLogger
	walLog          *wal.Log
	blockDB         blockdb.BlockDB
	stateDB         statedb.StateDB
	historyDB       historydb.HistoryDB
	contractEventDB contracteventdb.ContractEventDB
	resultDB        resultdb.ResultDB
	txExistDB       txexistdb.TxExistDB
	commonDB        protocol.DBHandle
	bigFilterDB     bigfilterdb.BigFilterDB
	rwCache         rolling_window_cache.RollingWindowCache
}

func newStoreBuilder(chainId string, config *conf.StorageConfig,
	logger protocol.Logger, p11Handle *pkcs11.P11Handle) *storeBuilder {
	b := &storeBuilder{
		chainId:   chainId,
		config:    config,
		logger:    logger,
		p11Handle: p11Handle,
		dbFactory: dbprovider.NewDBFactory(),
	}
	b.registerAllKnownCreators()
	return b
}

// registerAllKnownCreators registers all known creators for the db components.
func (b *storeBuilder) registerAllKnownCreators() {
	b.registerCreator((*blockdb.BlockDB)(nil), b.toCreator(func() (interface{}, error) {
		return b.newBlockDB()
	}))
	b.registerCreator((*statedb.StateDB)(nil), b.toCreator(func() (interface{}, error) {
		return b.newStateDB()
	}))
	b.registerCreator((*resultdb.ResultDB)(nil), b.toCreator(func() (interface{}, error) {
		if b.config.DisableResultDB {
			return nil, nil
		}
		return b.newResultDB()
	}))
	b.registerCreator((*txexistdb.TxExistDB)(nil), b.toCreator(func() (interface{}, error) {
		return b.newTxExistKvDB()
	}))
	b.registerCreator((*historydb.HistoryDB)(nil), b.toCreator(func() (interface{}, error) {
		if b.config.DisableHistoryDB {
			return nil, nil
		}
		return b.newHistoryDB()
	}))
	b.registerCreator((*contracteventdb.ContractEventDB)(nil), b.toCreator(func() (interface{}, error) {
		if b.config.DisableContractEventDB {
			return nil, nil
		}
		return b.newContractEventDB()
	}))
	b.registerCreator((*localDB)(nil), b.toCreator(func() (interface{}, error) {
		return b.newLocalDB()
	}))
	b.registerCreator((**wal.Log)(nil), b.toCreator(func() (interface{}, error) {
		if !b.config.DisableBlockFileDb {
			return nil, nil
		}
		return b.newWal()
	}))
	b.registerCreator((*binlog.BinLogger)(nil), b.toCreator(func() (interface{}, error) {
		if b.config.DisableBlockFileDb {
			return nil, nil
		}
		return b.newBlockFile()
	}))
	b.registerCreator((*bigfilterdb.BigFilterDB)(nil), b.toCreator(func() (interface{}, error) {
		if !b.config.EnableBigFilter {
			return nil, nil
		}
		return b.newBigFilter()
	}))
	b.registerCreator((*rolling_window_cache.RollingWindowCache)(nil), b.toCreator(func() (interface{}, error) {
		return b.newRollingWindowCache()
	}))
}

// registerCreator map the underlying type of the elem to the instanceCreator.
func (s *storeBuilder) registerCreator(elem interface{}, creator instanceCreator) {
	if s.instanceCreator == nil {
		s.instanceCreator = make(map[reflect.Type]instanceCreator)
	}
	rt := reflect.TypeOf(elem)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	s.instanceCreator[rt] = creator
}

// toCreator transfer fn to instanceCreator.
func (s *storeBuilder) toCreator(fn func() (interface{}, error)) instanceCreator {
	return func(out interface{}) error {
		v, err := fn()
		// if v == nil return, otherwise ValueOf(nil).Type() or TypeOf(nil) will panic
		if err != nil || v == nil {
			return err
		}
		if !reflect.TypeOf(v).AssignableTo(reflect.TypeOf(out).Elem()) {
			return fmt.Errorf("%s not assignable to %s",
				reflect.TypeOf(v).Name(), reflect.TypeOf(out).Elem().Name())
		}

		reflect.ValueOf(out).Elem().Set(reflect.ValueOf(v))
		return nil
	}
}

// create a new instance and assign it to out, so out must be a pointer.
func (s *storeBuilder) create(out interface{}) error {
	rt := reflect.TypeOf(out)
	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("out must be a pointer, not a value")
	}
	creator, exist := s.instanceCreator[rt.Elem()]
	if !exist {
		return fmt.Errorf("can not find creator for %s", rt.Elem().Name())
	}
	return creator(out)
}

// Build firstly build all db components, if an error occurs close components constructed successfully previously
// then return error. Then build the store instance with all components.
func (b *storeBuilder) Build() (protocol.BlockchainStore, error) {
	dbConfig := b.config.BlockDbConfig
	if strings.ToLower(dbConfig.Provider) == "simple" {
		return b.newDebugStore(conf.DbconfigProviderLeveldb)
	}
	if strings.ToLower(dbConfig.Provider) == "memory" {
		return b.newDebugStore(conf.DbconfigProviderMemdb)
	}

	type (
		closer interface {
			Close()
		}
		closerRetError interface {
			Close() error
		}
	)
	var (
		storeImp     protocol.BlockchainStore
		err          error
		closeFns     = make([]func(), 0, 10)
		closeIfError = func(close func()) {
			closeFns = append(closeFns, close)
		}
		instancesCreated = []interface{}{
			&b.walLog,
			&b.binLogger,
			&b.blockDB,
			&b.stateDB,
			&b.historyDB,
			&b.contractEventDB,
			&b.resultDB,
			&b.txExistDB,
			&b.commonDB,
			&b.bigFilterDB,
			&b.rwCache,
		}
	)

	if err = b.initEncrypto(); err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			for _, fn := range closeFns {
				fn()
			}
		}
	}()

	for _, instance := range instancesCreated {
		if err = b.create(instance); err != nil {
			return nil, err
		}
		rv := reflect.ValueOf(instance)
		if rv.Elem().IsNil() {
			continue
		}
		itf := rv.Elem().Interface()
		if clr, ok := itf.(closer); ok {
			closeIfError(clr.Close)
		} else if clr, ok := itf.(closerRetError); ok {
			closeIfError(func() { clr.Close() })
		}
	}

	storeImp, err = NewBlockStoreImpl(
		b.chainId,
		b.config,
		b.blockDB,
		b.stateDB,
		b.historyDB,
		b.contractEventDB,
		b.resultDB,
		b.txExistDB,
		b.commonDB,
		b.logger,
		b.binLogger,
		b.walLog,
		b.bigFilterDB,
		b.rwCache,
	)
	if err == nil && b.config.Async {
		storeImp = NewAsyncBlockStoreImpl(storeImp, b.logger)
	}
	return storeImp, err
}

// initEncrypto build encryptor if configured.
func (b *storeBuilder) initEncrypto() error {
	if len(b.config.Encryptor) > 0 && len(b.config.EncryptKey) > 0 {
		encryptor, err := buildEncryptor(b.config.Encryptor, b.config.EncryptKey, b.p11Handle)
		if err != nil {
			return err
		}
		b.symmetricKey = encryptor
	}
	return nil
}

func (b *storeBuilder) newDebugStore(providerLevel string) (*test.DebugStore, error) {
	db, err := b.dbFactory.NewKvDB(b.chainId, providerLevel, StoreBlockDBDir,
		b.config.BlockDbConfig.LevelDbConfig, b.logger, nil)

	if err != nil {
		return nil, err
	}

	return test.NewDebugStore(b.logger, b.config, db), nil
}

func (b *storeBuilder) newBlockDB() (blockdb.BlockDB, error) {
	var (
		storeConfig   = b.config
		blockDBConfig = b.config.BlockDbConfig
		dir           = StoreBlockDBDir
		name          = DBName_BlockDB
		dbPrefix      = b.config.DbPrefix
	)

	if blockDBConfig.IsKVDB() {
		dbHandler, err := b.newKVDBHandler(blockDBConfig, dir, name, dbPrefix)
		if err != nil {
			return nil, err
		}
		if storeConfig.DisableBlockFileDb {
			return blockkvdb.NewBlockKvDB(b.chainId, dbHandler, b.logger, storeConfig), nil
		}
		return blockfiledb.NewBlockFileDB(b.chainId, dbHandler, b.logger, b.config, b.binLogger), nil
	}
	dbName := getDbName(dbPrefix, name, b.chainId)
	dbHandler, err := b.newSqlDBHandle(blockDBConfig, dbName)
	if err != nil {
		return nil, err
	}
	return blocksqldb.NewBlockSqlDB(dbName, dbHandler, b.logger), nil
}

func (b *storeBuilder) newStateDB() (statedb.StateDB, error) {
	var (
		stateDBConfig = b.config.StateDbConfig
		dir           = StoreStateDBDir
		name          = DBName_StateDB
		dbPrefix      = b.config.DbPrefix
	)

	if stateDBConfig.IsKVDB() {
		dbHandler, err := b.newKVDBHandler(stateDBConfig, dir, name, dbPrefix)
		if err != nil {
			return nil, err
		}
		if !b.config.DisableStateCache {
			newHandler, err := addCacheForDBHandler(b.config.StateCache, dbHandler, b.logger)
			if err != nil {
				dbHandler.Close()
				return nil, err
			}
			dbHandler = newHandler
		}
		return statekvdb.NewStateKvDB(b.chainId, dbHandler, b.logger, b.config), nil
	}
	dbName := getDbName(dbPrefix, name, b.chainId)
	dbHandler, err := b.newSqlDBHandle(stateDBConfig, dbName)
	if err != nil {
		return nil, err
	}
	//处理sqldb
	newDbFunc := func(dbName string) (protocol.SqlDBHandle, error) {
		return b.newSqlDBHandle(stateDBConfig, dbName)
	}
	//配置默认连接池连接数
	connPoolSize := 90
	if maxConnSize, ok := stateDBConfig.SqlDbConfig["max_open_conns"]; ok {
		connPoolSize, _ = maxConnSize.(int)
	}
	return statesqldb.NewStateSqlDB(b.config.DbPrefix, b.chainId, dbHandler, newDbFunc, b.logger, connPoolSize)
}

func (b *storeBuilder) newHistoryDB() (historydb.HistoryDB, error) {
	var (
		historyDBConfig = b.config.HistoryDbConfig
		dir             = StoreHistoryDBDir
		name            = DBName_HistoryDB
		dbPrefix        = b.config.DbPrefix
	)
	if historyDBConfig.IsKVDB() {
		//historyDB不使用缓存
		dbHandler, err := b.newKVDBHandler(&historyDBConfig.DbConfig, dir, name, dbPrefix)
		if err != nil {
			return nil, err
		}
		return historykvdb.NewHistoryKvDB(b.chainId, historyDBConfig, dbHandler, b.logger), nil
	}
	dbName := getDbName(dbPrefix, name, b.chainId)
	sqlDBHandler, err := b.newSqlDBHandle(&historyDBConfig.DbConfig, dbName)
	if err != nil {
		return nil, err
	}
	return historysqldb.NewHistorySqlDB(dbName, historyDBConfig, sqlDBHandler, b.logger), nil
}

func (b *storeBuilder) newResultDB() (resultdb.ResultDB, error) {
	var (
		resultDBConf = b.config.ResultDbConfig
		dir          = StoreResultDBDir
		name         = DBName_ResultDB
		dbPrefix     = b.config.DbPrefix
	)
	if resultDBConf.IsKVDB() {
		kvDBHandler, err := b.newKVDBHandler(resultDBConf, dir, name, dbPrefix)
		if err != nil {
			return nil, err
		}
		if b.config.DisableBlockFileDb {
			return resultkvdb.NewResultKvDB(b.chainId, kvDBHandler, b.logger, b.config), nil
		}
		// binlogger, err := b.newBlockFile()
		// if err != nil {
		// 	return nil, err
		// }
		return resultfiledb.NewResultFileDB(b.chainId, kvDBHandler, b.logger, b.config, b.binLogger), nil
	}
	dbName := getDbName(dbPrefix, name, b.chainId)
	sqlDBHandler, err := b.newSqlDBHandle(resultDBConf, dbName)
	if err != nil {
		return nil, err
	}
	return resultsqldb.NewResultSqlDB(dbName, sqlDBHandler, b.logger), nil
}

func (b *storeBuilder) newContractEventDB() (contracteventdb.ContractEventDB, error) {
	var (
		contractDBConf = b.config.ContractEventDbConfig
		name           = DBName_EventDB
		dbPrefix       = b.config.DbPrefix
	)
	dbName := getDbName(dbPrefix, name, b.chainId)
	sqlDBHandler, err := b.newSqlDBHandle(contractDBConf, dbName)
	if err != nil {
		return nil, err
	}
	return eventsqldb.NewContractEventDB(name, sqlDBHandler, b.logger)
}

func (b *storeBuilder) newTxExistKvDB() (txexistdb.TxExistDB, error) {
	var (
		txExistDBConf = b.config.TxExistDbConfig
		dir           = StoreTxExistDbDir
		name          = DBName_TxExistDB
		dbPrefix      = b.config.DbPrefix
	)
	if txExistDBConf == nil {
		return WrapBlockDB2TxExistDB(b.blockDB, b.logger), nil
	}
	if txExistDBConf.IsKVDB() {
		kvDBHandler, err := b.newKVDBHandler(txExistDBConf, dir, name, dbPrefix)
		if err != nil {
			return nil, err
		}
		return txexistkvdb.NewTxExistKvDB(b.chainId, kvDBHandler, b.logger), nil
	}
	return nil, nil
}

func (b *storeBuilder) newLocalDB() (protocol.DBHandle, error) {
	var (
		defaultDBConf = b.config.GetDefaultDBConfig()
		dir           = StoreLocalDBDir
		name          = DBName_LocalDB
		dbPrefix      = b.config.DbPrefix
	)
	if defaultDBConf.IsKVDB() {
		return b.newKVDBHandler(defaultDBConf, dir, name, dbPrefix)
	}
	dbName := getDbName(dbPrefix, name, b.chainId)
	return b.newSqlDBHandle(defaultDBConf, dbName)
}

func (b *storeBuilder) newBlockFile() (*blockfiledb.BlockFile, error) {
	storeConfig := b.config
	opts := blockfiledb.DefaultOptions
	opts.NoCopy = true
	opts.NoSync = storeConfig.LogDBSegmentAsync
	if storeConfig.LogDBSegmentSize > 64 { // LogDBSegmentSize default is 64MB
		opts.SegmentSize = storeConfig.LogDBSegmentSize * 1024 * 1024
	}
	if storeConfig.DisableLogDBMmap {
		opts.UseMmap = false
	}

	if storeConfig.ReadBFDBTimeOut > 0 {
		opts.ReadTimeOut = storeConfig.ReadBFDBTimeOut
	}

	bfdbPath := filepath.Join(storeConfig.StorePath, b.chainId, blockFilePath)
	tmpbfdbPath := ""
	if len(storeConfig.BlockStoreTmpPath) > 0 {
		tmpbfdbPath = filepath.Join(storeConfig.BlockStoreTmpPath, b.chainId, blockFilePath)
	}
	return blockfiledb.Open(bfdbPath, tmpbfdbPath, opts, b.logger)
}

func (b *storeBuilder) newWal() (*wal.Log, error) {
	storeConfig := b.config
	opts := wal.DefaultOptions
	opts.NoCopy = true
	opts.NoSync = storeConfig.LogDBSegmentAsync
	if storeConfig.LogDBSegmentSize > 0 {
		// LogDBSegmentSize default is 20MB
		opts.SegmentSize = storeConfig.LogDBSegmentSize * 1024 * 1024
	}

	walPath := filepath.Join(storeConfig.StorePath, b.chainId, walLogPath)
	return wal.Open(walPath, opts)
}

func (b *storeBuilder) newKVDBHandler(dbConfig *conf.DbConfig, dbDir,
	name, dbPrefix string) (protocol.DBHandle, error) {
	dbName := dbDir
	if dbConfig.Provider == conf.DbconfigProviderSqlKV {
		dbName = getDbName(dbPrefix, name, b.chainId)
	}
	config := dbConfig.GetDbConfig()
	return b.dbFactory.NewKvDB(b.chainId, dbConfig.Provider, dbName, config, b.logger, b.symmetricKey)
}

func (b *storeBuilder) newSqlDBHandle(dbConfig *conf.DbConfig, dbName string) (protocol.SqlDBHandle, error) {
	return b.dbFactory.NewSqlDB(b.chainId, dbName, dbConfig.SqlDbConfig, b.logger)
}

func (b *storeBuilder) newBigFilter() (bigfilterdb.BigFilterDB, error) {
	return newBigFilter(b.config.BigFilter, b.logger, b.chainId)
}

func (b *storeBuilder) newRollingWindowCache() (rolling_window_cache.RollingWindowCache, error) {
	return newRollingWindowCache(b.config, b.logger)
}

func addCacheForDBHandler(cacheConf *conf.CacheConfig,
	dbHandler protocol.DBHandle, logger protocol.Logger) (protocol.DBHandle, error) {
	c, err := newBigCache(cacheConf)
	if err != nil {
		return nil, err
	}
	return cache.NewCacheWrapToDBHandle(c, dbHandler, logger), nil
}
