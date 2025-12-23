/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package rawsqlprovider

import "errors"

var (
	//ErrSql sql error
	ErrSql = errors.New("sql error")
	//ErrSqlQuery sql query error
	ErrSqlQuery = errors.New("sql query error")
	//ErrTransaction database transaction error
	ErrTransaction = errors.New("database transaction error")
	//ErrConnection database connect error
	ErrConnection = errors.New("database connect error")
	//ErrDatabase database operation error
	ErrDatabase = errors.New("database operation error")
	//ErrTable table operation error
	ErrTable = errors.New("table operation error")
	//ErrRow table row query error
	ErrRow = errors.New("table row query error")
	//ErrIO database I/O error
	ErrIO = errors.New("database I/O error")
	//ErrTxNotFound transaction not found
	ErrTxNotFound = errors.New("transaction not found")
	//ErrTypeConvert type convert error
	ErrTypeConvert = errors.New("type convert error")
	//ErrNoRowExist no row exist
	ErrNoRowExist = errors.New("no row exist")
)
