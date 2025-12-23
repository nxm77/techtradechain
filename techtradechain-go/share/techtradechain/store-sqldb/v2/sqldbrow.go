/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package rawsqlprovider

import (
	"database/sql"
)

// SqlDBRow 封装sql.Rows
type SqlDBRow struct {
	rows *sql.Rows
}

// NewSqlDBRow 根据sql.Rows创建SqlDBRow
// @param row
// @return *SqlDBRow
func NewSqlDBRow(row *sql.Rows) *SqlDBRow {
	return &SqlDBRow{
		rows: row,
	}
}

// ScanColumns 读取列
// @param dest
// @return error
func (r *SqlDBRow) ScanColumns(dest ...interface{}) error {
	defer r.rows.Close()
	err := r.rows.Scan(dest...)
	if err != nil {
		return ErrRow
	}
	return nil
}

// Data 读取数据为map对象
// @return map[string][]byte key为列名，value为字段值
// @return error
func (row *SqlDBRow) Data() (map[string][]byte, error) {
	defer row.rows.Close()
	return convertRows2Map(row.rows)
}

// IsEmpty 是否空
// @return bool
func (row *SqlDBRow) IsEmpty() bool {
	return false
}

// emptyRow 表示空的对象
type emptyRow struct {
}

// ScanColumns 扫描列
// @param dest
// @return error
func (r *emptyRow) ScanColumns(dest ...interface{}) error {
	return nil
}

// Data 空的map
// @return map[string][]byte
// @return error
func (row *emptyRow) Data() (map[string][]byte, error) {
	return make(map[string][]byte), nil
}

// IsEmpty 返回真
// @return bool
func (row *emptyRow) IsEmpty() bool {
	return true
}

// SqlDBRows 封装多行的sql.Rows
type SqlDBRows struct {
	rows  *sql.Rows
	close func() error
}

// NewSqlDBRows 新建SqlDBRows对象
// @param rows
// @param close
// @return *SqlDBRows
func NewSqlDBRows(rows *sql.Rows, close func() error) *SqlDBRows {
	return &SqlDBRows{
		rows:  rows,
		close: close,
	}
}

// Next 是否有下一个值
// @return bool
func (r *SqlDBRows) Next() bool {
	return r.rows.Next()
}

// Close 关闭
// @return error
func (r *SqlDBRows) Close() error {
	rClose := r.rows.Close()
	if rClose != nil {
		return rClose
	}
	if r.close != nil {
		return r.close()
	}
	return nil
}

// ScanColumns 扫描列
// @param dest
// @return error
func (r *SqlDBRows) ScanColumns(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

// Data 返回的数据转换为map
// @return map[string][]byte
// @return error
func (r *SqlDBRows) Data() (map[string][]byte, error) {
	return convertRows2Map(r.rows)
}

func convertRows2Map(rows *sql.Rows) (map[string][]byte, error) {

	cols, err := rows.Columns()
	if err != nil {
		return nil, ErrRow
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	err = rows.Scan(scanArgs...)
	if err != nil {
		return nil, ErrRow
	}
	var value string
	resultC := map[string][]byte{}
	for i, col := range values {
		if col == nil {
			value = ""
		} else {
			value = string(col)
		}
		resultC[cols[i]] = []byte(value)
	}
	return resultC, nil
}

//func (r *SqlDBRows) ScanObject(dest interface{}) error {
//	return r.rows.Scan(dest)
//}
