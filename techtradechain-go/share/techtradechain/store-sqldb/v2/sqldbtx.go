/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package rawsqlprovider

import (
	"database/sql"
	"sync"
	"time"

	"techtradechain.com/techtradechain/protocol/v2"
)

//SqlDBTx 数据库事务的封装
type SqlDBTx struct {
	sync.Mutex
	name      string
	dbType    string
	db        *sql.Tx
	logger    protocol.Logger
	startTime time.Time
}

// NewSqlDBTx 创建一个SqlDBTx事务
// @param name
// @param dbType
// @param db
// @param logger
// @return *SqlDBTx
func NewSqlDBTx(name string, dbType string, db *sql.Tx, logger protocol.Logger) *SqlDBTx {
	return &SqlDBTx{
		name:      name,
		dbType:    dbType,
		db:        db,
		logger:    logger,
		startTime: time.Now(),
	}
}

// ChangeContextDb 修改当前上下文的数据库
// @param dbName
// @return error
func (p *SqlDBTx) ChangeContextDb(dbName string) error {
	if dbName == "" {
		return nil
	}
	p.Lock()
	defer p.Unlock()
	if p.dbType == DbType_Sqlite { //不支持切换数据库
		return nil
	}
	sqlStr := "use " + dbName
	p.logger.Debug("DBTx:", p.name, "Exec sql:", sqlStr)
	_, err := p.db.Exec(sqlStr)
	if err != nil {
		p.logger.Warnf("change context db fail, error: %s", err)
		return ErrTransaction
	}
	return nil
}

//SaveBatch 批量的保存同一种对象的多个实例
func (p *SqlDBTx) SaveBatch(vals []interface{}) (int64, error) {
	if len(vals) == 0 {
		return 0, nil
	}
	p.Lock()
	defer p.Unlock()
	val0 := vals[0]
	userDefineSave, ok := val0.(UserDefineSave)
	if !ok {
		count := int64(0)
		for _, val := range vals {
			c, err := p.save(val)
			if err != nil {
				return 0, err
			}
			count += c
		}
		return count, nil
	}
	//mysql 先做Prepare再insert
	prepareSql, _ := userDefineSave.GetSaveSql(p.dbType)
	p.logger.Debug("DBTx:", p.name, "Prepare sql:", prepareSql)
	stmt, err := p.db.Prepare(prepareSql)
	defer func() {
		closeErr := stmt.Close()
		if closeErr != nil {
			p.logger.Error("close stmt error:", closeErr)
		}
	}()
	if err != nil {
		return 0, err
	}
	count := int64(0)
	for _, val := range vals {
		_, data := val.(UserDefineSave).GetSaveSql(p.dbType)
		//p.logger.Debug("DBTx:", p.name, "batch save value:", data)
		result, err := stmt.Exec(data...)
		if err != nil {
			return 0, err
		}
		c, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		count += c
	}
	return count, nil
}

// Save 保存一个对象到数据库
// @param val
// @return int64
// @return error
func (p *SqlDBTx) Save(val interface{}) (int64, error) {
	p.Lock()
	defer p.Unlock()
	return p.save(val)
}

func (p *SqlDBTx) save(val interface{}) (int64, error) {
	userDefineSave, ok := val.(UserDefineSave)
	if ok {
		saveSql, args := userDefineSave.GetSaveSql(p.dbType)
		p.logger.Debug("DBTx:", p.name, "Exec sql:", saveSql, args)
		result, err := p.db.Exec(saveSql, args...)
		if err != nil {
			p.logger.Error(err)
			return 0, ErrSql
		}
		return result.RowsAffected()
	}

	value, ok := val.(TableDMLGenerator)
	if !ok {
		p.logger.Errorf("%v not a TableDMLGenerator", val)
		return 0, ErrTypeConvert
	}

	countSql, args := value.GetCountSql()
	p.logger.Debug("DBTx:", p.name, "Query sql:", countSql, args)
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
		p.logger.Debug("DBTx:", p.name, "Exec sql:", insert, args)
		result, err := p.db.Exec(insert, args...)
		if err != nil {
			p.logger.Warn("Exec sql:", insert, args)
			p.logger.Error(err)
			return 0, ErrSql
		}
		rowCount, err = result.RowsAffected()
		if err != nil {
			p.logger.Error(err)
			return 0, ErrSql
		}
	} else {
		update, args := value.GetUpdateSql()
		p.logger.Debug("DBTx:", p.name, "Exec sql:", update, args)
		effect, err := p.db.Exec(update, args...)
		if err != nil {
			p.logger.Error(err)
			return 0, ErrSql
		}
		rowCount, err = effect.RowsAffected()
		if err != nil {
			p.logger.Error(err)
			return 0, ErrSql
		}
	}
	return rowCount, nil
}

// ExecSql 执行SQL语句
// @param sql
// @param values
// @return int64
// @return error
func (p *SqlDBTx) ExecSql(sql string, values ...interface{}) (int64, error) {
	p.Lock()
	defer p.Unlock()
	tx, err := p.db.Exec(sql, values...)
	p.logger.Debugf("DBTx:%s exec sql[%s] %v,result:%v", p.name, sql, values, err)
	if err != nil {
		p.logger.Error(err)
		return 0, ErrSql
	}
	rowCount, err := tx.RowsAffected()
	if err != nil {
		p.logger.Error(err)
		return 0, ErrSql
	}
	return rowCount, nil
}

// QuerySingle 单行查询
// @param sql
// @param values
// @return protocol.SqlRow
// @return error
func (p *SqlDBTx) QuerySingle(sql string, values ...interface{}) (protocol.SqlRow, error) {
	p.Lock()
	defer p.Unlock()
	db := p.db
	p.logger.Debug("DBTx:", p.name, "Query sql:", sql, values)
	rows, err := db.Query(sql, values...)
	if err != nil {
		p.logger.Error(err)
		return nil, ErrSqlQuery
	}
	if !rows.Next() {
		return &emptyRow{}, nil
	}
	return NewSqlDBRow(rows), nil
}

// QueryMulti 多行查询
// @param sql
// @param values
// @return protocol.SqlRows
// @return error
func (p *SqlDBTx) QueryMulti(sql string, values ...interface{}) (protocol.SqlRows, error) {
	p.Lock()
	defer p.Unlock()
	p.logger.Debug("DBTx:", p.name, "Query sql:", sql, values)
	rows, err := p.db.Query(sql, values...)
	if err != nil {
		p.logger.Error(err)
		return nil, ErrSqlQuery
	}
	return NewSqlDBRows(rows, nil), nil
}

// Commit 提交事务
// @return error
func (p *SqlDBTx) Commit() error {
	p.Lock()
	defer p.Unlock()
	err := p.db.Commit()
	p.logger.Debugf("commit tx[%s], tx duration:%s", p.name, time.Since(p.startTime).String())
	if err != nil {
		p.logger.Error(err)
		return ErrTransaction
	}
	return nil
}

// Rollback 回滚事务
// @return error
func (p *SqlDBTx) Rollback() error {
	p.Lock()
	defer p.Unlock()
	err := p.db.Rollback()
	p.logger.Warnf("rollback tx[%s], tx duration:%s", p.name, time.Since(p.startTime).String())
	if err != nil {
		p.logger.Error(err)
		return ErrTransaction
	}
	return nil
}

// BeginDbSavePoint 开启保存点
// @param spName
// @return error
func (p *SqlDBTx) BeginDbSavePoint(spName string) error {
	p.Lock()
	defer p.Unlock()
	savePointName := getSavePointName(spName)
	_, err := p.db.Exec("SAVEPOINT " + savePointName)
	p.logger.Debugf("db tx[%s] new savepoint[%s],result:%s", p.name, savePointName, err)
	if err != nil {
		p.logger.Error(err)
		return ErrTransaction
	}
	return nil
}

// RollbackDbSavePoint 回滚保存点
// @param spName
// @return error
func (p *SqlDBTx) RollbackDbSavePoint(spName string) error {
	p.Lock()
	defer p.Unlock()
	savePointName := getSavePointName(spName)
	_, err := p.db.Exec("ROLLBACK TO SAVEPOINT " + savePointName)
	p.logger.Infof("db tx[%s] rollback savepoint[%s],result:%s", p.name, savePointName, err)
	if err != nil {
		p.logger.Error(err)
		return ErrTransaction
	}
	return nil
}
func getSavePointName(spName string) string {
	return "SP_" + spName
}
