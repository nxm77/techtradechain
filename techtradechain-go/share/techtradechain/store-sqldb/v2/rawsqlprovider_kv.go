/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package rawsqlprovider

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/google/uuid"
)

var _ protocol.DBHandle = (*SqlDBHandle)(nil)

//KeyValue KV object
type KeyValue struct {
	ObjectKey   []byte `gorm:"size:128;primaryKey;default:''"`
	ObjectValue []byte `gorm:"type:longblob"`
}

const putKvSql = "REPLACE INTO key_values(`object_key`,`object_value`) VALUES(?, ?)"
const deleteSql = "DELETE FROM key_values WHERE object_key=?"

// GetCreateTableSql get a create KV table SQL
// @param dbType mysql/sqlite/tdsql
// @return string sql string
func (kv *KeyValue) GetCreateTableSql(dbType string) string {
	if dbType == DbType_MySQL {
		return "CREATE TABLE `key_values` (`object_key` varchar(767) primary key, `object_value` longblob) " +
			"default character set utf8mb4"
	} else if dbType == DbType_Sqlite {
		return "CREATE TABLE `key_values` (`object_key` varbinary(2000), `object_value` longblob)"
	} else if dbType == DbType_TDSQL {
		return "CREATE TABLE `key_values` (`object_key` varchar(767) primary key, `object_value` longblob) " +
			"shardkey=object_key"
	}
	panic("Unsupported db type:" + dbType)
}

// GetTableName key_values
// @return string
func (kv *KeyValue) GetTableName() string {
	return "key_values"
}

// GetSaveSql get a save kv sql
// @param dbType:mysql/sqlite/tdsql
// @return string sql
// @return []interface{} key,value
func (kv *KeyValue) GetSaveSql(dbType string) (string, []interface{}) {
	if dbType == DbType_Sqlite {
		return "INSERT OR REPLACE INTO key_values(`object_key`,`object_value`) VALUES(?, ?)",
			[]interface{}{kv.ObjectKey, kv.ObjectValue}
	}
	return putKvSql, []interface{}{kv.ObjectKey, kv.ObjectValue}
}

// GetInsertSql get a insert kv sql
// @param dbType:mysql/sqlite/tdsql
// @return string sql
// @return []interface{} key,value
func (kv *KeyValue) GetInsertSql(dbType string) (string, []interface{}) {
	if dbType == DbType_Sqlite {
		return "INSERT INTO key_values(`object_key`,`object_value`) VALUES(?, ?) ",
			[]interface{}{kv.ObjectKey, kv.ObjectValue}
	}
	return "INSERT INTO key_values(`object_key`,`object_value`) " +
			"VALUES(?, ?) " +
			"ON DUPLICATE KEY UPDATE `object_value`=?",
		[]interface{}{kv.ObjectKey, kv.ObjectValue, kv.ObjectValue}
}

// GetUpdateSql get an update sql
// @return string sql
// @return []interface{} value,key
func (kv *KeyValue) GetUpdateSql() (string, []interface{}) {
	return "UPDATE key_values SET object_value=? WHERE object_key=?", []interface{}{kv.ObjectValue, kv.ObjectKey}
}

// GetCountSql get a count filter by key sql
// @return string sql
// @return []interface{} key
func (kv *KeyValue) GetCountSql() (string, []interface{}) {
	return "SELECT count(*) FROM key_values WHERE  object_key=?", []interface{}{kv.ObjectKey}
}

// NewKVDBHandle construct a new SqlDBHandle
func NewKVDBHandle(input *NewSqlDBOptions) *SqlDBHandle {
	dbName := input.DbName
	conf := input.Config
	log := input.Logger
	provider := &SqlDBHandle{dbTxCache: make(map[string]*SqlDBTx), log: log, encryptor: input.Encryptor}
	sqlType, err := ParseSqlDbType(conf.SqlDbType)
	if err != nil {
		log.Panic(err.Error())
	}
	provider.dbType = sqlType
	if sqlType == DbType_MySQL || sqlType == DbType_TDSQL {
		dsn := replaceMySqlDsn(conf.Dsn, dbName)
		if sqlType == DbType_TDSQL {
			//fix bug about TDSQL not support large column value
			dsn = dsn + "&maxAllowedPacket=167772160"
		}
		db, err1 := sql.Open(DbType_MySQL, dsn)
		if err1 != nil {
			log.Panic("connect to mysql error:" + err.Error())
		}
		err = db.Ping()
		//_, err = db.Query("SELECT DATABASE()")
		if err != nil {
			if strings.Contains(err.Error(), "Unknown database") {
				log.Infof("first time connect to a new database,create database %s", dbName)
				err = provider.createDatabase(conf.Dsn, dbName)
				if err != nil {
					log.Panicf("failed to open mysql[%s] and create database %s, %s", dsn, dbName, err)
				}
				db, err = sql.Open(DbType_MySQL, dsn)
				if err != nil {
					log.Panicf("failed to open mysql:%s , %s", dsn, err)
				}
			} else {
				log.Panicf("failed to open mysql:%s , %s", dsn, err)
			}
		}
		log.Debug("open new db connection for " + conf.SqlDbType + " dsn:" + dsn)
		if conf.ConnMaxLifeTime > 0 {
			defaultConnMaxLifeTime = conf.ConnMaxLifeTime
		}
		if conf.MaxIdleConns > 0 {
			defaultMaxIdleConns = conf.MaxIdleConns
		}
		if conf.MaxOpenConns > 0 {
			defaultMaxOpenConns = conf.MaxOpenConns
		}
		db.SetConnMaxLifetime(time.Second * time.Duration(defaultConnMaxLifeTime))
		db.SetMaxIdleConns(defaultMaxIdleConns)
		db.SetMaxOpenConns(defaultMaxOpenConns)
		provider.db = db
		provider.contextDbName = dbName //默认连接mysql数据库
		err = provider.CreateTableIfNotExist(&KeyValue{})
		if err != nil {
			log.Panic(err.Error())
		}
	} else if sqlType == DbType_Sqlite {
		dbPath := conf.Dsn
		if !strings.Contains(dbPath, ":memory:") { //不是内存数据库模式，则需要在路径中包含chainId
			dbPath = filepath.Join(dbPath, dbName)
			err1 := provider.createDirIfNotExist(dbPath)
			if err1 != nil {
				log.Panicf("failed to create folder for sqlite path:%s,get error:%s", dbPath, err1)
			}
			dbPath = filepath.Join(dbPath, "sqlite.db")
		}
		log.Debug("open a sqlite connect for path:", dbPath)
		db, err2 := sql.Open("sqlite3", dbPath)
		if err2 != nil {
			log.Panicf("failed to open sqlite path:%s,get error:%s", dbPath, err2)
		}
		provider.db = db
	} else {
		log.Panicf("unsupported db:%v", sqlType)
	}

	log.Debug("inject TechTradeChain logger into db logger.")
	provider.log = log
	provider.contextDbName = dbName
	err = provider.CreateTableIfNotExist(&KeyValue{})
	if err != nil {
		log.Panic(err.Error())
	}
	return provider
}

//GetDbType returns db type
func (p *SqlDBHandle) GetDbType() string {
	return DbType_MySQL
}

// Get returns the value for the given key, or returns nil if none exists
func (p *SqlDBHandle) Get(key []byte) ([]byte, error) {
	sql := "SELECT object_value FROM key_values WHERE object_key=?"
	var result protocol.SqlRow
	var err error
	//TDSQL 的Key是string类型才能分片
	if p.dbType == DbType_TDSQL {
		result, err = p.QuerySingle(sql, string(key))
	} else {
		result, err = p.QuerySingle(sql, key)
	}
	if err != nil {
		return nil, err
	}
	if result.IsEmpty() {
		p.log.Debugf("cannot query value by key=%s", string(key))
		return nil, nil
	}
	var v []byte
	err = result.ScanColumns(&v)
	if err != nil {
		return nil, err
	}
	if p.encryptor != nil && len(v) > 0 {
		return p.encryptor.Decrypt(v)
	}
	return v, nil
}

// GetKeys returns the value for the given key
func (p *SqlDBHandle) GetKeys(keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	keysArr := []interface{}{}
	for i := 0; i < len(keys); i++ {
		if keys[i] == nil {
			continue
		}
		keysArr = append(keysArr, keys[i])
	}
	// get subArgs:?,?,?
	subArgs := ""
	for i := 0; i < len(keysArr); i++ {
		if i == 0 {
			subArgs = subArgs + "?"
		} else {
			subArgs = subArgs + ",?"
		}
	}

	// SELECT object_value FROM key_values WHERE object_key in (?,?,?)
	sql := "SELECT object_key,object_value FROM key_values WHERE object_key in (" + subArgs + ")"
	//var result protocol.SqlRows
	var err error
	dataMap := make(map[string][]byte)

	rows, err := p.QueryMulti(sql, keysArr...)
	//rows, err := p.QueryMulti(sql, start, limit)
	if err != nil {
		p.log.Errorf("query multi error,errInfo:[%s]", err)
		return nil, err
	}
	result := newSqlKVIterator(rows, p.encryptor)
	for result.Next() {
		k := result.Key()
		v := result.Value()
		dataMap[string(k)] = v
	}

	res := [][]byte{}
	for i := 0; i < len(keys); i++ {
		if keys[i] == nil {
			res = append(res, nil)
			continue
		}
		v, exist := dataMap[string(keys[i])]
		//not exsit
		if !exist {
			res = append(res, nil)
			continue
		}
		//exist
		copyData := make([]byte, len(v))
		copy(copyData, v)
		res = append(res, copyData)
	}

	return res, nil
}

// Put saves the key-values
func (p *SqlDBHandle) Put(key []byte, value []byte) error {
	//针对mysql特殊优化 TODO 不一定所有MySQL都支持
	//if p.dbType == DbType_MySQL {
	//	insertSql := "INSERT INTO key_values values(?, ?) ON DUPLICATE KEY UPDATE object_value=VALUES(?)"
	//	p.log.DebugDynamic(func() string {
	//		return fmt.Sprintf("Exec sql:%s,key:%x,value:%x", insertSql, key, value)
	//	})
	//	result, err := p.db.Exec(insertSql, key, value, value)
	//	if err != nil {
	//		return errSql
	//	}
	//	var rowCount int64
	//	rowCount, err = result.RowsAffected()
	//	if err != nil || rowCount == 0 {
	//		return errSql
	//	}
	//	return nil
	//}
	//非MySQL，走传统的Save模式
	kv := &KeyValue{
		ObjectKey:   key,
		ObjectValue: value,
	}
	if p.encryptor != nil && len(kv.ObjectValue) > 0 {
		var err error
		kv.ObjectValue, err = p.encryptor.Encrypt(kv.ObjectValue)
		if err != nil {
			return err
		}
	}
	_, err := p.Save(kv)
	return err
}

// Has return true if the given key exist, or return false if none exists
func (p *SqlDBHandle) Has(key []byte) (bool, error) {
	sql := "SELECT count(*) FROM key_values WHERE object_key=?"
	result, err := p.QuerySingle(sql, key)
	if err != nil {
		return false, err
	}
	var count int
	err = result.ScanColumns(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// Delete deletes the given key
func (p *SqlDBHandle) Delete(key []byte) error {
	count, err := p.ExecSql(deleteSql, key)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNoRowExist
	}
	return nil
}
func deleteInTx(tx protocol.SqlDBTransaction, key []byte, l protocol.Logger) error {
	count, err := tx.ExecSql(deleteSql, key)
	if err != nil {
		return err
	}
	if count == 0 {
		l.Warnf("no row exist delete by key=%x", key)
	}
	return nil
}

// WriteBatch writes a batch in an atomic operation
func (p *SqlDBHandle) WriteBatch(batch protocol.StoreBatcher, sync bool) error {
	txName := fmt.Sprintf("Tx%s%d", uuid.New().String(), time.Now().UnixNano())
	tx, err := p.db.Begin()
	start := time.Now()
	defer func() {
		p.log.Debugf("process batch in dbtx[%s] spend time:%v", txName, time.Since(start))
	}()
	//p.log.Debugf("txName=%s", txName)
	if err != nil {
		return err
	}
	//saveData := make([]interface{}, 0)

	stmt, err := tx.Prepare(putKvSql)
	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			p.log.Error("close stmt error:", closeErr)
		}
	}()
	if err != nil {
		return err
	}
	count := int64(0)

	for k, v := range batch.KVs() {
		key := []byte(k)
		if v == nil {
			delResult, err1 := tx.Exec(deleteSql, key)
			if err1 != nil {
				if err2 := tx.Rollback(); err2 != nil {
					p.log.Errorf("rollback db transaction[%s] get an error:%s", txName, err2)
				}
				return err1
			}
			delCount, _ := delResult.RowsAffected()
			if delCount == 0 {
				p.log.Warnf("no row exist delete by key=%x", key)
			}
		} else {
			kv := &KeyValue{key, v}
			if p.encryptor != nil && len(kv.ObjectValue) > 0 {
				kv.ObjectValue, err = p.encryptor.Encrypt(kv.ObjectValue)
				if err != nil {
					return err
				}
			}
			//saveData = append(saveData, kv)

			result, err := stmt.Exec(kv.ObjectKey, kv.ObjectValue)
			if err != nil {
				p.log.Warnf("try to rollback db transaction[%s] since %s", txName, err)
				if err2 := tx.Rollback(); err2 != nil {
					p.log.Errorf("rollback db transaction[%s] get an error:%s", txName, err2)
				}
				return err
			}
			c, err := result.RowsAffected()
			if err != nil {
				p.log.Warnf("try to rollback db transaction[%s] since %s", txName, err)
				if err2 := tx.Rollback(); err2 != nil {
					p.log.Errorf("rollback db transaction[%s] get an error:%s", txName, err2)
				}
				return err
			}
			count += c
		}
	}
	p.log.Debugf("saveBatch count:%d", count)
	return tx.Commit()
}

// NewIteratorWithRange returns an iterator that contains all the key-values between given key ranges
// start is included in the results and limit is excluded.
func (p *SqlDBHandle) NewIteratorWithRange(start []byte, limit []byte) (protocol.Iterator, error) {
	if len(start) == 0 || len(limit) == 0 {
		return nil, fmt.Errorf("iterator range should not start(%s) or limit(%s) with empty key",
			string(start), string(limit))
	}
	sql := "SELECT * FROM key_values WHERE object_key >= ? AND object_key < ?"
	rows, err := p.QueryMulti(sql, start, limit)
	if err != nil {
		return nil, nil
	}
	result := newSqlKVIterator(rows, p.encryptor)
	return result, nil
}

// NewIteratorWithPrefix returns an iterator that contains all the key-values with given prefix
func (p *SqlDBHandle) NewIteratorWithPrefix(prefix []byte) (protocol.Iterator, error) {
	if len(prefix) == 0 {
		return nil, fmt.Errorf("iterator prefix should not be empty key")
	}

	sql := "SELECT * FROM key_values WHERE object_key LIKE ?"
	arg := append(append([]byte{}, prefix...), '%')
	rows, err := p.QueryMulti(sql, arg)
	if err != nil {
		return nil, nil
	}
	result := newSqlKVIterator(rows, p.encryptor)
	return result, nil
}

// GetWriteBatchSize return write batch size
func (p *SqlDBHandle) GetWriteBatchSize() uint64 {
	if p.writeBatchSize <= 0 {
		return 1000
	}
	return p.writeBatchSize

}

type sqlkvIterator struct {
	rows      protocol.SqlRows
	err       error
	kv        *KeyValue
	encryptor crypto.SymmetricKey
}

func newSqlKVIterator(rows protocol.SqlRows, encryptor crypto.SymmetricKey) *sqlkvIterator {
	return &sqlkvIterator{rows: rows, encryptor: encryptor}
}
func (i *sqlkvIterator) Next() bool {
	i.kv = nil

	return i.rows.Next()
}
func (i *sqlkvIterator) First() bool {
	return false //sql不支持移动到First
}
func (i *sqlkvIterator) Error() error {
	return i.err
}
func (i *sqlkvIterator) Key() []byte {
	return i.getKV().ObjectKey
}
func (i *sqlkvIterator) Value() []byte {
	value := i.getKV().ObjectValue
	if i.encryptor != nil && len(value) > 0 {
		v, err := i.encryptor.Decrypt(value)
		if err != nil {
			i.err = err
		}
		return v
	}
	return value
}
func (i *sqlkvIterator) getKV() KeyValue {
	if i.kv == nil {
		i.kv = &KeyValue{}
		i.err = i.rows.ScanColumns(&i.kv.ObjectKey, &i.kv.ObjectValue)
	}
	return *i.kv
}
func (i *sqlkvIterator) Release() {
	i.rows.Close()
}
