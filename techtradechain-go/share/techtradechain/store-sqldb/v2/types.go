/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package rawsqlprovider

// TableDDLGenerator 创建数据库表的接口
type TableDDLGenerator interface {
	// GetCreateTableSql 获得创建表的SQL语句
	// @param dbType
	// @return string
	GetCreateTableSql(dbType string) string
	// GetTableName 获得表的名字
	// @return string
	GetTableName() string
}

// TableDMLGenerator 对象数据在表中增改的接口
type TableDMLGenerator interface {
	// GetInsertSql 获得插入一行数据到表的SQL语句和参数
	// @param dbType
	// @return string
	// @return []interface{}
	GetInsertSql(dbType string) (string, []interface{})
	// GetUpdateSql 获得更新一条数据行对应的SQL语句与参数
	// @return string
	// @return []interface{}
	GetUpdateSql() (string, []interface{})
	// GetCountSql 获得该对象在数据库中存在的行数的SQL和参数
	// @return string
	// @return []interface{}
	GetCountSql() (string, []interface{})
}

// UserDefineSave 用户自定义了保存一个对象的SQL语句的方法
type UserDefineSave interface {
	// GetSaveSql 提供一个保存该对象的SQL语句和参数列表
	//  @param dbType
	//  @return string
	//  @return []interface{}
	GetSaveSql(dbType string) (string, []interface{})
}

// DbType_MySQL mysql
const DbType_MySQL = "mysql"

// DbType_TDSQL tdsql
const DbType_TDSQL = "tdsql"

// DbType_Sqlite sqlite
const DbType_Sqlite = "sqlite"
