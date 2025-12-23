/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/sdk-go/v2/archive-mysql/model"
	"techtradechain.com/techtradechain/sdk-go/v2/archive-mysql/mysql"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ArchiveService 归档服务
var _ ArchiveService = (*ArchiveMySqlClient)(nil)

// ArchiveMySqlClient 归档服务的MySQL客户端实现
type ArchiveMySqlClient struct {
	// db is the database connection
	db *gorm.DB
	// cc is the chain client
	cc SDKInterface
	// secretKey is the secret key for encrypting data
	secretKey string
	// log is the logger
	log utils.Logger
}

// NewArchiveMySqlClient creates a new ArchiveMySqlClient
// @param chainId
// @param config
// @param cc
// @param log
// @return *ArchiveMySqlClient
func NewArchiveMySqlClient(chainId string, config *ArchiveConfig, cc SDKInterface,
	log utils.Logger) *ArchiveMySqlClient {
	return NewArchiveMySqlClient2(chainId, config.Dest, config.secretKey, cc, log)
}

// NewArchiveMySqlClient2 creates a new ArchiveMySqlClient
// @param chainId
// @param dbDest
// @param secretKey
// @param cc
// @param log
// @return *ArchiveMySqlClient
func NewArchiveMySqlClient2(chainId, dbDest string, secretKey string, cc SDKInterface,
	log utils.Logger) *ArchiveMySqlClient {
	db, err := initDb(chainId, dbDest, log)
	if err != nil {
		panic(err)
	}
	return &ArchiveMySqlClient{
		db:        db,
		cc:        cc,
		secretKey: secretKey,
		log:       log,
	}
}

// initDb Connecting database, migrate tables.
func initDb(chainId string, dbDest string, l utils.Logger) (*gorm.DB, error) {
	// parse params
	dbName := model.DbName(chainId)
	dbDestSlice := strings.Split(dbDest, ":")
	if len(dbDestSlice) != 4 {
		return nil, errors.New("invalid database destination")
	}

	// initialize database
	db, err := mysql.InitDb(dbDestSlice[0], dbDestSlice[1], dbDestSlice[2], dbDestSlice[3], dbName, true)
	if err != nil {
		return nil, err
	}
	db.Logger = newSqlLogger(l)
	// migrate sysinfo table
	err = db.AutoMigrate(&model.Sysinfo{})
	if err != nil {
		return nil, err
	}
	return db, nil
}

// GetTxByTxId get transaction by txId
// @param txId
// @return *common.TransactionInfo
// @return error
func (a *ArchiveMySqlClient) GetTxByTxId(txId string) (*common.TransactionInfo, error) {
	blkHeight, err := a.cc.GetBlockHeightByTxId(txId)
	if err != nil {
		return nil, err
	}
	bInfo, err := a.GetBlockByHeight(blkHeight, false)
	if err != nil {
		return nil, err
	}
	txInfo, err := a.getTxByTxIdInBlock(bInfo, txId, false)
	if err != nil {
		return nil, err
	}
	return &common.TransactionInfo{
		Transaction:    txInfo.Transaction,
		BlockHeight:    txInfo.BlockHeight,
		BlockHash:      txInfo.BlockHash,
		TxIndex:        txInfo.TxIndex,
		BlockTimestamp: txInfo.BlockTimestamp,
	}, nil
}

func (a *ArchiveMySqlClient) getTxByTxIdInBlock(blkWithRWSet *common.BlockInfo, txId string, witRWSet bool) (
	*common.TransactionInfoWithRWSet, error) {
	for idx, tx := range blkWithRWSet.Block.Txs {
		if tx.Payload.TxId == txId {
			txInfo := &common.TransactionInfoWithRWSet{
				Transaction: tx,
				BlockHeight: uint64(blkWithRWSet.Block.Header.BlockHeight),
				BlockHash:   blkWithRWSet.Block.Header.BlockHash,
				TxIndex:     uint32(idx),
			}
			if witRWSet {
				txInfo.RwSet = blkWithRWSet.RwsetList[idx]
			}
			return txInfo, nil
		}
	}
	return nil, errors.New("tx not found")
}

// GetTxWithRWSetByTxId get transaction with rwset by txId
// @param txId
// @return *common.TransactionInfoWithRWSet
// @return error
func (a *ArchiveMySqlClient) GetTxWithRWSetByTxId(txId string) (*common.TransactionInfoWithRWSet, error) {
	blkHeight, err := a.cc.GetBlockHeightByTxId(txId)
	if err != nil {
		return nil, err
	}
	bInfo, err := a.GetBlockByHeight(blkHeight, true)
	if err != nil {
		return nil, err
	}
	return a.getTxByTxIdInBlock(bInfo, txId, true)
}

// GetBlockByHeight get block by height
// @param height
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveMySqlClient) GetBlockByHeight(height uint64, withRWSet bool) (*common.BlockInfo, error) {
	var bInfo model.BlockInfo
	err := a.db.Table(model.BlockInfoTableNameByBlockHeight(height)).
		Where("Fblock_height=? AND Fis_archived=1", height).
		First(&bInfo).Error
	if err != nil {
		return nil, err
	}
	var blkWithRWSetOffChain store.BlockWithRWSet
	err = blkWithRWSetOffChain.Unmarshal(bInfo.BlockWithRWSet)
	if err != nil {
		return nil, err
	}
	result := &common.BlockInfo{
		Block: blkWithRWSetOffChain.Block,
	}
	if withRWSet {
		result.RwsetList = blkWithRWSetOffChain.TxRWSets
	}
	return result, nil

}

// GetBlockByHash get block by hash
// @param blockHash
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveMySqlClient) GetBlockByHash(blockHash string, withRWSet bool) (*common.BlockInfo, error) {
	height, err := a.cc.GetBlockHeightByHash(blockHash)
	if err != nil {
		return nil, err
	}
	return a.GetBlockByHeight(height, withRWSet)
}

// GetBlockByTxId get block by txId
// @param txId
// @param withRWSet
// @return *common.BlockInfo
// @return error
func (a *ArchiveMySqlClient) GetBlockByTxId(txId string, withRWSet bool) (*common.BlockInfo, error) {
	height, err := a.cc.GetBlockHeightByTxId(txId)
	if err != nil {
		return nil, err
	}
	return a.GetBlockByHeight(height, withRWSet)
}

// GetChainConfigByBlockHeight get chain config by block height
// @param blockHeight
// @return *config.ChainConfig
// @return error
func (a *ArchiveMySqlClient) GetChainConfigByBlockHeight(blockHeight uint64) (*config.ChainConfig, error) {
	offChainHeight, _, _, err := a.GetArchivedStatus()
	if err != nil {
		return nil, err
	}
	if blockHeight <= offChainHeight {
		blkHeader, blkErr := a.GetBlockByHeight(blockHeight, false)
		if blkErr != nil {
			return nil, blkErr
		}
		if blkHeader.Block.Header.BlockType == common.BlockType_CONFIG_BLOCK {
			return getChainConfig(blkHeader.Block.Txs[0])
		}
		configBlk, configBlkErr := a.GetBlockByHeight(blkHeader.Block.Header.PreConfHeight, true)
		if configBlkErr != nil {
			return nil, configBlkErr
		}
		return getChainConfig(configBlk.Block.Txs[0])
	}
	return nil, nil
}

// Register register archive client
// @param genesis
// @return error
func (a *ArchiveMySqlClient) Register(genesis *common.BlockInfo) error {
	a.log.Debug("create table sysinfo")
	err := a.db.AutoMigrate(&model.Sysinfo{})
	if err != nil {
		return err
	}
	err = model.InitArchiveStatusData(a.db)
	if err != nil {
		return err
	}
	a.log.Debug("create table t_block_info_1")
	err = a.db.AutoMigrate(&model.BlockInfo{})
	if err != nil {
		return err
	}

	return a.ArchiveBlock(genesis)
}

// ArchiveBlock 归档一个区块
// @param block
// @return error
func (a *ArchiveMySqlClient) ArchiveBlock(block *common.BlockInfo) error {
	// lock database
	locker := mysql.NewDbLocker(a.db, "cmc", mysql.DefaultLockLeaseAge)
	locker.Lock()
	defer locker.UnLock()
	// create block info table
	if block.Block.Header.BlockHeight%model.RowsPerBlockInfoTable() == 0 {
		err := model.CreateBlockInfoTableIfNotExists(a.db,
			model.BlockInfoTableNameByBlockHeight(block.Block.Header.BlockHeight))
		if err != nil {
			return err
		}
	}
	// check if this block info was already in database
	var bInfo model.BlockInfo
	tx := a.db.Begin()
	err := tx.Table(model.BlockInfoTableNameByBlockHeight(block.Block.Header.BlockHeight)).
		Where("Fblock_height = ?", block.Block.Header.BlockHeight).First(&bInfo).Error
	if err == nil { // this block info was already in database, just update Fis_archived to 1
		if !bInfo.IsArchived {
			bInfo.IsArchived = true
			a.db.Table(model.BlockInfoTableNameByBlockHeight(block.Block.Header.BlockHeight)).Save(&bInfo)
		}
	} else if err == gorm.ErrRecordNotFound {
		// this block info was not in database, insert it
		blkWithRWSet := &store.BlockWithRWSet{
			Block:          block.Block,
			TxRWSets:       block.RwsetList,
			ContractEvents: nil,
		}
		// marshal block info
		blkWithRWSetBytes, err := blkWithRWSet.Marshal()
		if err != nil {
			return err
		}
		// calculate hash
		sum, err := hmac(block.Block.Header.ChainId, blkWithRWSet.Block.Header.BlockHeight,
			blkWithRWSetBytes, a.secretKey)
		if err != nil {
			return err
		}
		// insert block info
		err = model.InsertBlockInfo(tx, block.Block.Header.ChainId, blkWithRWSet.Block.Header.BlockHeight,
			blkWithRWSetBytes, sum)
		if err != nil {
			return err
		}
		// update archived block height off-chain
		err = model.UpdateArchivedBlockHeight(tx, blkWithRWSet.Block.Header.BlockHeight)
		if err != nil {
			return err
		}
		return tx.Commit().Error

	} else {
		return err
	}
	return nil
}

// ArchiveBlocks 归档多个区块
// @param bi
// @param heightNoticeCallback
// @return error
func (a *ArchiveMySqlClient) ArchiveBlocks(bi BlockIterator, heightNoticeCallback func(ProcessMessage) error) error {
	sendCount := 0
	notice := func(h uint64, e error) {
		if heightNoticeCallback != nil {
			heightNoticeCallback(ProcessMessage{
				CurrentHeight: h,
				Total:         bi.Total(),
				Error:         e,
			})
		}
	}
	for bi.Next() {
		block, err1 := bi.Value()
		if err1 != nil {
			notice(bi.Current(), err1)
			return err1
		}
		err := a.ArchiveBlock(block)
		if err != nil {
			notice(block.Block.Header.BlockHeight, err)
			return fmt.Errorf("send height %d got error %s",
				block.Block.Header.BlockHeight, err.Error())
		}
		sendCount++
		notice(block.Block.Header.BlockHeight, nil)
	}
	if sendCount == 0 {
		return errors.New("no block to archive")
	}
	return nil
}

// GetArchivedStatus get archived status
// @return archivedHeight
// @return inArchive
// @return code
// @return err
func (a *ArchiveMySqlClient) GetArchivedStatus() (archivedHeight uint64, inArchive bool, code uint32, err error) {
	archivedBlkHeightOffChain, err := model.GetArchivedBlockHeight(a.db)
	if err != nil {
		return 0, false, 0, errors.New("chain genesis not exists")
	}
	return archivedBlkHeightOffChain, false, 0, nil
}

// Close 关闭与归档数据源的链接
// @return err
func (a *ArchiveMySqlClient) Close() error {
	if a.db != nil {
		db, err := a.db.DB()
		if err != nil {
			return err
		}
		return db.Close()
	}
	return nil
}

// hmac SM3(Fchain_id+Fblock_height+Fblock_with_rwset+key)
func hmac(chainId string, blkHeight uint64, blkWithRWSetBytes []byte, secretKey string) (string, error) {
	blkHeightBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(blkHeightBytes, blkHeight)

	var data []byte
	data = append(data, []byte(chainId)...)
	data = append(data, blkHeightBytes...)
	data = append(data, blkWithRWSetBytes...)
	data = append(data, []byte(secretKey)...)
	return SM3(data)
}

// SM3 sum of data in SM3, returns sum hex
func SM3(data []byte) (string, error) {
	bz, err := hash.Get(crypto.HASH_TYPE_SM3, data)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(bz), nil
}

type sqlLogger struct {
	logger utils.Logger
}

func (s *sqlLogger) LogMode(level logger.LogLevel) logger.Interface {
	return s
}

func (s *sqlLogger) Info(ctx context.Context, s2 string, i ...interface{}) {
	s.logger.Infof(s2, i...)
}

func (s *sqlLogger) Warn(ctx context.Context, s2 string, i ...interface{}) {
	s.logger.Warnf(s2, i...)
}

func (s *sqlLogger) Error(ctx context.Context, s2 string, i ...interface{}) {
	s.logger.Errorf(s2, i...)
}

func (s *sqlLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sql, rows := fc()
	if rows == -1 {
		s.logger.Warnf("Trace begin: %s, end: %s, err: %s, sql: %s", begin, time.Now(), err, sql)
	} else {
		s.logger.Infof("Trace begin: %s, end: %s, err: %s, sql: %s, rows: %d", begin, time.Now(), err, sql, rows)
	}
}

var _ logger.Interface = (*sqlLogger)(nil)

// newSqlLogger wrap Logger to gorm Logger
// @param l
// @return *sqlLogger
func newSqlLogger(l utils.Logger) *sqlLogger {
	return &sqlLogger{logger: l}
}
