/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rawsqlprovider

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/protocol/v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

var _ protocol.SqlDBHandle = (*SqlDBHandle)(nil)

var defaultMaxIdleConns = 10
var defaultMaxOpenConns = 10
var defaultConnMaxLifeTime = 60

// SqlDBHandle 核心DBHandle实现对象
type SqlDBHandle struct {
	sync.RWMutex
	contextDbName  string
	db             *sql.DB
	dbType         string //DbType_Sqlite DbType_MySQL
	dbTxCache      map[string]*SqlDBTx
	log            protocol.Logger
	writeBatchSize uint64
	encryptor      crypto.SymmetricKey
}

// CompactRange 压缩，无实现
// @param start
// @param limit
// @return error
func (p *SqlDBHandle) CompactRange(start, limit []byte) error {
	return errors.New("no implement for sql db")
}

// ParseSqlDbType 解析数据库类型
// @param str
// @return string
// @return error
func ParseSqlDbType(str string) (string, error) {
	switch strings.ToLower(str) {
	case DbType_Sqlite:
		return DbType_Sqlite, nil
	case DbType_MySQL:
		return DbType_MySQL, nil
	case DbType_TDSQL:
		return DbType_TDSQL, nil
	default:
		return "", errors.New("unknown sql db type:" + str)
	}
}

// Utf8Char UTF8
const Utf8Char = "charset=utf8mb4"

// DsnParsetime parseTime=True
const DsnParsetime = "parseTime=True"

func replaceMySqlDsn(dsn string, dbName string) string {
	dsnPattern := regexp.MustCompile(
		`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
			`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
			`\/(?P<dbname>.*?)` + // /dbname
			`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]
	matches := dsnPattern.FindStringSubmatchIndex(dsn)
	if len(matches) < 12 {
		return dsn
	}
	start, end := matches[10], matches[11]
	newDsn := dsn[:start] + dbName + dsn[end:]
	if matches[12] == -1 {
		return newDsn + "?" + Utf8Char + "&" + DsnParsetime
	}
	par := dsn[matches[12]:]
	if !strings.Contains(par, "charset=") {
		newDsn = newDsn + "&" + Utf8Char
	}
	if !strings.Contains(par, "parseTime=") {
		newDsn = newDsn + "&" + DsnParsetime
	}
	return newDsn
}

// NewSqlDBOptions Option对象
type NewSqlDBOptions struct {
	Config    *SqlDbConfig
	Logger    protocol.Logger
	Encryptor crypto.SymmetricKey
	ChainId   string
	DbName    string
}

// NewSqlDBHandle construct a new SqlDBHandle
func NewSqlDBHandle(input *NewSqlDBOptions) *SqlDBHandle {
	dbName := input.DbName
	conf := input.Config
	log := input.Logger
	provider := &SqlDBHandle{dbTxCache: make(map[string]*SqlDBTx), log: log}
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
		db, err := sql.Open(DbType_MySQL, dsn)
		if err != nil {
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
	} else if sqlType == DbType_Sqlite {
		dbPath := conf.Dsn
		if !strings.Contains(dbPath, ":memory:") { //不是内存数据库模式，则需要在路径中包含chainId
			dbPath = filepath.Join(dbPath, dbName)
			err := provider.createDirIfNotExist(dbPath)
			if err != nil {
				log.Panicf("failed to create folder for sqlite path:%s,get error:%s", dbPath, err)
			}
			dbPath = filepath.Join(dbPath, "sqlite.db")
		}
		log.Debug("open a sqlite connect for path:", dbPath)
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			log.Panicf("failed to open sqlite path:%s,get error:%s", dbPath, err)
		}
		provider.db = db
	} else {
		log.Panicf("unsupported db:%v", sqlType)
	}

	log.Debug("inject TechTradeChain logger into db logger.")
	provider.log = log
	provider.contextDbName = dbName
	return provider
}

//NewMemSqlDBHandle for unit test
func NewMemSqlDBHandle(log protocol.Logger) *SqlDBHandle {
	provider := &SqlDBHandle{dbTxCache: make(map[string]*SqlDBTx), log: log}
	db, _ := sql.Open("sqlite3", ":memory:")
	provider.db = db
	provider.dbType = DbType_Sqlite
	return provider
}

//NewShareMemSqlDBHandle share cache
func NewShareMemSqlDBHandle(log protocol.Logger) *SqlDBHandle {
	provider := &SqlDBHandle{dbTxCache: make(map[string]*SqlDBTx), log: log}
	db, _ := sql.Open("sqlite3", "file:memdb1?mode=memory&cache=shared")
	provider.db = db
	provider.dbType = DbType_Sqlite
	return provider
}

// GetSqlDbType 获得对象的数据库类型
// @return string
func (p *SqlDBHandle) GetSqlDbType() string {
	return p.dbType
}
func (p *SqlDBHandle) createDatabase(dsn string, dbName string) error {
	db, err := sql.Open(DbType_MySQL, dsn)
	if err != nil {
		p.log.Error(err)
		return ErrConnection
	}
	defer db.Close()
	sqlStr := buildCreateDatabaseSql(dbName)
	_, err = db.Exec(sqlStr)
	p.log.Debug("Exec sql:", sqlStr)
	if err != nil {
		p.log.Error(err)
		return ErrDatabase
	}
	return nil
}
func buildCreateDatabaseSql(dbName string) string {
	return "CREATE DATABASE " + dbName + " DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_general_ci"
}
func (p *SqlDBHandle) createDirIfNotExist(path string) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		// 创建文件夹
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			p.log.Error(err)
			return ErrIO
		}
	}
	return nil
}

//CreateDatabaseIfNotExist 如果数据库不存在则创建对应的数据库，创建后将当前数据库设置为新数据库，返回是否已存在
func (p *SqlDBHandle) CreateDatabaseIfNotExist(dbName string) (bool, error) {
	p.Lock()
	defer p.Unlock()
	if p.contextDbName == dbName {
		return true, nil
	}
	if p.dbType == DbType_Sqlite {
		return true, nil
	}
	//尝试切换数据库
	_, err := p.db.Exec("use " + dbName)
	p.log.Debugf("DB %s try to run 'use database %s' change context db", p.contextDbName, dbName)
	if err != nil { //切换失败，没有这个数据库，则创建
		p.log.Debugf("try to run 'use %s' get an error, it means database not exist, create it!", dbName)
		createDbSql := buildCreateDatabaseSql(dbName)
		_, err = p.db.Exec(createDbSql)
		if err != nil {
			p.log.Error(err)
			return false, ErrDatabase //创建失败
		}
		p.log.Info(createDbSql)
		//返回false表示数据库之前并不存在
		return false, nil
	}
	//切换成功，说明数据库已经存在，将当前连接的数据库切换回来
	_, err = p.db.Exec("use " + p.contextDbName)
	if err != nil {
		return false, err
	}
	p.log.Debugf("DB %s run 'use database %s' change context db back", p.contextDbName, p.contextDbName)
	return true, nil
}

//CreateTableIfNotExist 根据一个对象struct，自动构建对应的sql数据库表
func (p *SqlDBHandle) CreateTableIfNotExist(objI interface{}) error {
	p.Lock()
	defer p.Unlock()
	obj, ok := objI.(TableDDLGenerator)
	if !ok {
		p.log.Errorf("%v not a TableDDLGenerator", objI)
		return ErrTypeConvert
	}
	if !p.hasTable(obj) {
		return p.CreateTable(obj)
	}
	return nil
}
func (p *SqlDBHandle) hasTable(obj TableDDLGenerator) bool {
	hasTable, _ := p.HasTable(obj.GetTableName())
	return hasTable
}

// HasTable 判断某表是否已经存在
// @param tableName
// @return bool
// @return error
func (p *SqlDBHandle) HasTable(tableName string) (bool, error) {
	//obj:=objI.(TableDDLGenerator)
	sql := ""
	if p.dbType == DbType_MySQL || p.dbType == DbType_TDSQL {
		sql = fmt.Sprintf(
			`SELECT count(*) 
FROM information_schema.tables 
WHERE table_schema = '%s' AND table_name = '%s' AND table_type = 'BASE TABLE'`,
			p.contextDbName, tableName)
	}
	if p.dbType == DbType_Sqlite {
		sql = fmt.Sprintf(`SELECT count(*) 
FROM sqlite_master 
WHERE type='table' AND name='%s'`, tableName)
	}
	p.log.Debug("DB", p.contextDbName, "Query sql:", sql)
	row := p.db.QueryRow(sql)
	count := 0
	err := row.Scan(&count)
	if err != nil {
		p.log.Error("scan count get error:%s", err)
		return false, err
	}
	return count > 0, nil
}

// CreateTable 创建一个对象对应的表
// @param obj
// @return error
func (p *SqlDBHandle) CreateTable(obj TableDDLGenerator) error {
	sql := obj.GetCreateTableSql(p.dbType)
	p.log.Debug("DB", p.contextDbName, "Exec ddl:", sql, "database:", p.contextDbName)
	_, err := p.db.Exec(sql)
	if err != nil {
		p.log.Error(err)
		return ErrTable //创建失败
	}
	return nil
}

//ExecSql 执行SQL语句
func (p *SqlDBHandle) ExecSql(sql string, values ...interface{}) (int64, error) {
	p.Lock()
	defer p.Unlock()
	p.log.Debug("DB", p.contextDbName, "Exec sql:", sql, values)
	tx, err := p.db.Exec(sql, values...)
	if err != nil {
		// todo optimization
		if strings.Contains(err.Error(), "doesn't exist") {
			p.log.Warnf(err.Error())
		} else {
			p.log.Error(err)
		}
		return 0, ErrSql
	}
	return tx.RowsAffected()
}

// Save 保存一个对象到数据库
// @param val
// @return int64
// @return error
func (p *SqlDBHandle) Save(val interface{}) (int64, error) {
	p.Lock()
	defer p.Unlock()
	value, ok := val.(TableDMLGenerator)
	if !ok {
		p.log.Errorf("%v not a TableDMLGenerator", val)
		return 0, ErrTypeConvert
	}
	countSql, args := value.GetCountSql()
	p.log.Debug("DB", p.contextDbName, "Query sql:", countSql, args)
	row := p.db.QueryRow(countSql, args...)
	if row.Err() != nil {
		return 0, row.Err()
	}
	rowCount := int64(0)
	err := row.Scan(&rowCount)
	if err != nil {
		return 0, ErrSql
	}
	if rowCount == 0 { //数据库不存在对应数据，执行Insert操作
		insert, args := value.GetInsertSql(p.dbType)
		p.log.Debug("DB", p.contextDbName, "Exec sql:", insert, args)
		result, err := p.db.Exec(insert, args...)
		if err != nil {
			return 0, ErrSql
		}
		rowCount, err = result.RowsAffected()
		if err != nil {
			return 0, ErrSql
		}
	} else { //数据库存在，执行Update操作
		update, args := value.GetUpdateSql()
		p.log.Debug("DB", p.contextDbName, "Exec sql:", update, args)
		effect, err := p.db.Exec(update, args...)
		if err != nil {
			return 0, ErrSql
		}
		rowCount, err = effect.RowsAffected()
		if err != nil {
			return 0, ErrSql
		}
	}

	return rowCount, nil
}

// QuerySingle 查询并返回单行记录
// @param sql
// @param values
// @return protocol.SqlRow
// @return error
func (p *SqlDBHandle) QuerySingle(sql string, values ...interface{}) (protocol.SqlRow, error) {
	p.RLock()
	defer p.RUnlock()
	db := p.db
	p.log.Debug("DB", p.contextDbName, "Query sql:", sql, values)
	rows, err := db.Query(sql, values...)
	if err != nil {
		// todo optimization
		// if table doesn't exist,the result will be empty rather than return error
		if strings.Contains(err.Error(), "doesn't exist") {
			p.log.Warnf(err.Error())
			return &emptyRow{}, nil
		}
		p.log.Error(err)
		return nil, ErrSqlQuery
	}

	if !rows.Next() {
		return &emptyRow{}, nil
	}
	return NewSqlDBRow(rows), nil
}

// QueryMulti 查询并返回多行记录
// @param sql
// @param values
// @return protocol.SqlRows
// @return error
func (p *SqlDBHandle) QueryMulti(sql string, values ...interface{}) (protocol.SqlRows, error) {
	p.RLock()
	defer p.RUnlock()
	p.log.Debug("DB", p.contextDbName, "Query sql:", sql, values)
	rows, err := p.db.Query(sql, values...)
	if err != nil {
		p.log.Error(err)
		return nil, ErrSqlQuery
	}
	return NewSqlDBRows(rows, nil), nil
}

// BeginDbTransaction 开启一个事务并命名该事务对象
// @param txName
// @return protocol.SqlDBTransaction
// @return error
func (p *SqlDBHandle) BeginDbTransaction(txName string) (protocol.SqlDBTransaction, error) {
	if p.dbType == DbType_Sqlite {
		p.Lock()
		defer p.Unlock()
		return p.beginDbTransaction(txName, true)
	}
	return p.beginDbTransaction(txName, true)
}

func (p *SqlDBHandle) beginDbTransaction(txName string, onlyOne bool) (protocol.SqlDBTransaction, error) {
	if p.dbType == DbType_Sqlite {
		if _, has := p.dbTxCache[txName]; has {
			return nil, errors.New("transaction already exist, please use GetDbTransaction to get it or commit/rollback it")
		}

		if onlyOne {
			if err := p.rollbackDbByTx(txName); err != nil {
				return nil, err
			}
		}
		tx, err := p.db.Begin()
		if err != nil {
			p.log.Error(err)
			return nil, ErrTransaction
		}
		sqltx := NewSqlDBTx(txName, p.dbType, tx, p.log)
		p.dbTxCache[txName] = sqltx
		p.log.Debugf("DB %s start new db transaction[%s]", p.contextDbName, txName)
		return sqltx, nil
	}
	p.Lock()
	if _, has := p.dbTxCache[txName]; has {
		p.Unlock()
		return nil, errors.New("transaction already exist, please use GetDbTransaction to get it or commit/rollback it")
	}
	p.Unlock()
	if onlyOne {
		if err := p.rollbackDbByTx(txName); err != nil {
			return nil, err
		}
	}
	tx, err := p.db.Begin()
	if err != nil {
		p.log.Error(err)
		return nil, ErrTransaction
	}
	sqltx := NewSqlDBTx(txName, p.dbType, tx, p.log)
	p.Lock()
	p.dbTxCache[txName] = sqltx
	p.Unlock()
	p.log.Debugf("DB %s start new db transaction[%s]", p.contextDbName, txName)
	return sqltx, nil
}

//func (p *SqlDBHandle) rollbackAllCacheDbTx(newTxName string) error {
//	for txKey, dbHandel := range p.dbTxCache {
//		p.log.Warnf("try to rollback dbtx[%s] since new db transaction[%s] start", txKey, newTxName)
//		err := dbHandel.Rollback()
//		if err != nil {
//			p.log.Errorf("rollback dbtx[%s] get an error:%s", txKey, err)
//			return err
//		}
//		delete(p.dbTxCache, txKey)
//	}
//	return nil
//}
func (p *SqlDBHandle) rollbackDbByTx(newTxName string) error {
	for txKey, dbHandel := range p.dbTxCache {
		if txKey == newTxName {
			p.log.Warnf("try to rollback dbtx[%s] since new db transaction[%s] start", txKey, newTxName)
			err := dbHandel.Rollback()
			if err != nil {
				p.log.Errorf("rollback dbtx[%s] get an error:%s", txKey, err)
				return err
			}
			delete(p.dbTxCache, txKey)
		}
	}
	return nil
}

// GetDbTransaction 根据名字获得之前创建的事务对象
// @param txName
// @return protocol.SqlDBTransaction
// @return error
func (p *SqlDBHandle) GetDbTransaction(txName string) (protocol.SqlDBTransaction, error) {
	p.Lock()
	defer p.Unlock()
	return p.getDbTransaction(txName)
}
func (p *SqlDBHandle) getDbTransaction(txName string) (*SqlDBTx, error) {
	tx, has := p.dbTxCache[txName]
	if !has {
		return nil, ErrTxNotFound
	}
	return tx, nil
}

// CommitDbTransaction 提交指定名字的事务
// @param txName
// @return error
func (p *SqlDBHandle) CommitDbTransaction(txName string) error {
	p.Lock()
	defer p.Unlock()
	tx, err := p.getDbTransaction(txName)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		p.log.Error(err)
		return ErrTransaction
	}
	delete(p.dbTxCache, txName)
	//p.log.Debugf("commit db transaction[%s]", txName) //devin: already log in tx.Commit()
	return nil
}

// RollbackDbTransaction 回滚指定名字的事务
// @param txName
// @return error
func (p *SqlDBHandle) RollbackDbTransaction(txName string) error {
	p.Lock()
	defer p.Unlock()
	tx, err := p.getDbTransaction(txName)
	if err != nil {
		return err
	}
	err = tx.Rollback()
	if err != nil {
		p.log.Error(err)
		return ErrTransaction
	}
	delete(p.dbTxCache, txName)
	//p.log.Debugf("rollback db transaction[%s]", txName) //devin: already log in tx.Rollback()
	return nil
}

// Close 关闭数据库连接
// @return error
func (p *SqlDBHandle) Close() error {
	p.Lock()
	defer p.Unlock()
	if len(p.dbTxCache) > 0 {
		txNames := ""
		for name, tx := range p.dbTxCache {
			txNames += name + ";"
			err := tx.Rollback()
			if err != nil {
				return err
			}
		}
		p.log.Warnf("these db tx[%s] don't commit or rollback, close them.", txNames)
	}
	err := p.db.Close()
	if err != nil {
		p.log.Error(err)
		return ErrConnection
	}
	return nil
}
