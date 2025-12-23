/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */
//
//store_factory 完成对存储对象的初始化，主要包括：
//	1.解析配置
//	2.根据配置文件创建 wal/bfdb、blockdb、statedb、resultdb、txExistdb、historydb、resultdb、cache、bigfilter 等对象
//	3.完成logger初始化
//	4.主要通过ioc 容器完成构造

package store

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	"techtradechain.com/techtradechain/store/v2/rolling_window_cache"

	"techtradechain.com/techtradechain/store/v2/bigfilterdb"
	"techtradechain.com/techtradechain/store/v2/bigfilterdb/bigfilterkvdb"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/pkcs11"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/aes"
	"techtradechain.com/techtradechain/common/v2/crypto/sym/sm4"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/store/v2/blockdb"
	"techtradechain.com/techtradechain/store/v2/cache"
	"techtradechain.com/techtradechain/store/v2/conf"
	"techtradechain.com/techtradechain/store/v2/serialization"
	"techtradechain.com/techtradechain/store/v2/txexistdb"
	"github.com/allegro/bigcache/v3"
)

// nolint
const (
	//StoreBlockDBDir blockdb folder name
	StoreBlockDBDir = "store_block"
	//StoreStateDBDir statedb folder name
	StoreStateDBDir = "store_state"
	//StoreHistoryDBDir historydb folder name
	StoreHistoryDBDir = "store_history"
	//StoreResultDBDir resultdb folder name
	StoreResultDBDir   = "store_result"
	StoreEventLogDBDir = "store_event_log"
	StoreLocalDBDir    = "localdb"
	StoreTxExistDbDir  = "store_txexist"

	DBName_BlockDB   = "blockdb"
	DBName_StateDB   = "statedb"
	DBName_HistoryDB = "historydb"
	DBName_ResultDB  = "resultdb"
	DBName_EventDB   = "eventdb"
	DBName_LocalDB   = "localdb"
	DBName_TxExistDB = "txexistdb"
)

// Factory is a factory function to create an instance of the block store
// which commits block into the ledger.
// @Description:
type Factory struct {
}

// NewFactory add next time
// @Description:
// @return *Factory
func NewFactory() *Factory {
	return &Factory{}
}

// NewStore constructs new BlockStore
// @Description:
// @receiver m
// @param chainId
// @param storeConfig
// @param logger
// @param p11Handle
// @return protocol.BlockchainStore
// @return error
func (m *Factory) NewStore(chainId string, storeConfig *conf.StorageConfig,
	logger protocol.Logger, p11Handle *pkcs11.P11Handle) (protocol.BlockchainStore, error) {
	return newStoreBuilder(chainId, storeConfig, logger, p11Handle).Build()
}

// buildEncryptor 构建加密
// @Description:
// @param encryptor
// @param key
// @param p11Handle
// @return crypto.SymmetricKey
// @return error
func buildEncryptor(encryptor, key string, p11Handle *pkcs11.P11Handle) (crypto.SymmetricKey, error) {
	switch strings.ToLower(encryptor) {
	case "sm4":
		if p11Handle != nil {
			return pkcs11.NewSecretKey(p11Handle, key, crypto.SM4)
		}
		return &sm4.SM4Key{Key: readEncryptKey(key)}, nil
	case "aes":
		if p11Handle != nil {
			return pkcs11.NewSecretKey(p11Handle, key, crypto.AES)
		}
		return &aes.AESKey{Key: readEncryptKey(key)}, nil
	default:
		return nil, errors.New("unsupported encryptor:" + encryptor)
	}
}

// readEncryptKey 读加密key
// @Description:
// @param key
// @return []byte
func readEncryptKey(key string) []byte {
	reg := regexp.MustCompile("^0[xX][0-9a-fA-F]+$") //is hex
	if reg.Match([]byte(key)) {
		b, _ := hex.DecodeString(key[2:])
		return b
	}

	removeFile := func(path string) {
		if errw := ioutil.WriteFile(path, []byte(""), 0600); errw != nil {
			fmt.Printf("remove encrypt key file: %s failed! you can manual remove. err: %v \n", key, errw)
		}
	}

	// 对于linux系统，临时文件以/开头
	// 对于windows系统，临时文件以C开头
	if (key[0] == '/' || key[0] == 'C') && pathExists(key) {
		f, err := ioutil.ReadFile(key)
		f1 := strings.Replace(string(f), " ", "", -1)
		f1 = strings.Replace(f1, "\n", "", -1)
		if err == nil {
			removeFile(key)
			return []byte(f1)
		}
	}

	return []byte(key)
}

// pathExists 判断目录是否存在
// @Description:
// @param path
// @return bool
func pathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return os.IsExist(err)
}

// newBigCache 创建 bigCache
// @Description:
// @param cacheConfig
// @return cache.Cache
// @return error
func newBigCache(cacheConfig *conf.CacheConfig) (cache.Cache, error) {
	bigCacheDefaultConfig := bigcache.Config{
		// number of shards (must be a power of 2)
		Shards: 1024,
		// time after which entry can be evicted
		LifeWindow: 10 * time.Minute,
		// rps * lifeWindow, used only in initial memory allocation
		CleanWindow: 10 * time.Second,
		// max entry size in bytes, used only in initial memory allocation
		MaxEntrySize: 500,
		// prints information about additional memory allocation
		Verbose: true,
		// cache will not allocate more memory than this limit, value in MB
		// if value is reached then the oldest entries can be overridden for the new ones
		// 0 value means no size limit
		HardMaxCacheSize: 128,
		// callback fired when the oldest entry is removed because of its
		// expiration time or no space left for the new entry. Default value is nil which
		// means no callback and it prevents from unwrapping the oldest entry.
		OnRemove: nil,
	}
	// 配置文件不为空，使用给定配置
	if cacheConfig != nil {
		bigCacheDefaultConfig.LifeWindow = cacheConfig.LifeWindow
		bigCacheDefaultConfig.CleanWindow = cacheConfig.CleanWindow
		bigCacheDefaultConfig.MaxEntrySize = cacheConfig.MaxEntrySize
		bigCacheDefaultConfig.HardMaxCacheSize = cacheConfig.HardMaxCacheSize
	}
	return bigcache.NewBigCache(bigCacheDefaultConfig)
}

// newBigFilter 创建一个bigFilter
// @Description:
// @param bigFilterConfig
// @param logger
// @param name
// @return bigfilterdb.BigFilterDB
// @return error
func newBigFilter(bigFilterConfig *conf.BigFilterConfig, logger protocol.Logger,
	name string) (bigfilterdb.BigFilterDB, error) {
	redisHosts := strings.Split(bigFilterConfig.RedisHosts, ",")
	filterNum := len(redisHosts)
	// 密码为空
	if bigFilterConfig.Pass == "" {
		b, err := bigfilterkvdb.NewBigFilterKvDB(filterNum, bigFilterConfig.TxCapacity, bigFilterConfig.FpRate,
			logger, redisHosts, nil, name)
		if err != nil {
			return b, err
		}
	}
	b, err := bigfilterkvdb.NewBigFilterKvDB(filterNum, bigFilterConfig.TxCapacity, bigFilterConfig.FpRate,
		logger, redisHosts, &bigFilterConfig.Pass, name)
	if err != nil {
		return b, err
	}
	return b, nil

}

// newRollingWindowCache
// @Description: 创建一个 RWCache
// @param storeConfig
// @param logger
// @return rolling_window_cache.RollingWindowCache
// @return error
func newRollingWindowCache(storeConfig *conf.StorageConfig,
	logger protocol.Logger) (rolling_window_cache.RollingWindowCache, error) {
	rollingWindowCacheCapacity := storeConfig.RollingWindowCacheCapacity
	// 未配置或者配置为0，则给默认 10000
	if storeConfig.RollingWindowCacheCapacity == 0 {
		rollingWindowCacheCapacity = 10000
	}
	r := rolling_window_cache.NewRollingWindowCacher(rollingWindowCacheCapacity,
		0, 0, 0, 0, logger)
	return r, nil

}

// getDbName
// @Description: db名字 由前缀、dbname、chainId组成，多链不重复
// @param dbPrefix
// @param dbName
// @param chainId
// @return string
func getDbName(dbPrefix, dbName, chainId string) string {
	return dbPrefix + dbName + "_" + chainId
}

// noTxExistDB
// @Description:
type noTxExistDB struct {
	db blockdb.BlockDB
}

// InitGenesis
// @Description: 初始化创世块
// @receiver n
// @param genesisBlock
// @return error
func (n noTxExistDB) InitGenesis(genesisBlock *serialization.BlockWithSerializedInfo) error {
	return nil
}

// CommitBlock
// @Description: 提交创世块
// @receiver n
// @param blockWithRWSet
// @param isCache
// @return error
func (n noTxExistDB) CommitBlock(blockWithRWSet *serialization.BlockWithSerializedInfo, isCache bool) error {
	return nil
}

// GetLastSavepoint
// @Description: 保存最后写
// @receiver n
// @return uint64
// @return error
func (n noTxExistDB) GetLastSavepoint() (uint64, error) {
	return n.db.GetLastSavepoint()
}

// TxExists
// @Description: 判断交易是否存在
// @receiver n
// @param txId
// @return bool
// @return error
func (n noTxExistDB) TxExists(txId string) (bool, error) {
	return n.db.TxExists(txId)
}

// Close
// @Description: 关闭db
// @receiver n
func (n noTxExistDB) Close() {

}

// WrapBlockDB2TxExistDB 创建空db
// @Description:
// @param db
// @param log
// @return txexistdb.TxExistDB
func WrapBlockDB2TxExistDB(db blockdb.BlockDB, log protocol.Logger) txexistdb.TxExistDB {
	log.Info("no TxExistDB config, use BlockDB to replace TxExistDB")
	return &noTxExistDB{db: db}
}
