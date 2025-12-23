// Copyright (C) BABEC. All rights reserved.
// Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

// Package mysql define mysql db operation
package mysql

import (
	"database/sql"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	dbMaxIdleConns         = 10
	dbMaxOpenConns         = 100
	dbConnMaxLifetime      = 0
	colonSeparator         = ":"
	tcpProtocolSeparator   = "@tcp("
	closeBracketsSeparator = ")/"
)

// InitDb Connect db server and create database if not exists then switch to this database,
// returns *gorm.DB, error
func InitDb(user, password, host, port, dbName string, migrateLock bool) (*gorm.DB, error) {
	// create database first.
	dsn := user + colonSeparator + password + tcpProtocolSeparator + host + colonSeparator + port + closeBracketsSeparator
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	_, err = sqlDB.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	if err != nil {
		return nil, err
	}
	if err := sqlDB.Close(); err != nil {
		return nil, err
	}

	// init gorm.DB instance
	dsn = user + colonSeparator + password + tcpProtocolSeparator + host + colonSeparator + port + closeBracketsSeparator +
		dbName + "?charset=utf8mb4&parseTime=True&loc=Local"
	return InitDbByDsn(dsn, migrateLock)
}

// InitDbByDsn Connect db server by dsn and returns *gorm.DB, error
// @param dsn
// @param migrateLock
// @return *gorm.DB
// @return error
func InitDbByDsn(dsn string, migrateLock bool) (*gorm.DB, error) {
	// init gorm.DB instance
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Silent),
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxIdleConns(dbMaxIdleConns)
	sqlDB.SetMaxOpenConns(dbMaxOpenConns)
	sqlDB.SetConnMaxLifetime(dbConnMaxLifetime)

	// migrate lock table
	if migrateLock {
		err = db.AutoMigrate(&lock{})
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}
