// Package binlog package
package binlog

/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import (
	"time"

	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
)

// BinLogger add next time
// @Description:
type BinLogger interface {
	// TruncateFront add next time
	//  @Description:
	//  @param index
	//  @return error
	TruncateFront(index uint64) error

	// ReadLastSegSection add next time
	//  @Description:
	//  @param index
	//  @return data
	//  @return fileName
	//  @return offset
	//  @return blkLen
	//  @return err
	ReadLastSegSection(index uint64, forceFetch bool) (data []byte, fileName string, offset, blkLen uint64, err error)

	// LastIndex add next time
	//  @Description:
	//  @return index
	//  @return err
	LastIndex() (index uint64, err error)

	// Write add next time
	//  @Description:
	//  @param index
	//  @param data
	//  @return fileName
	//  @return offset
	//  @return blkLen
	//  @return err
	Write(index uint64, data []byte) (fileName string, offset, blkLen uint64, err error)

	// ReadFileSection read data, according to file index
	// @Description:
	// @param fiIndex
	// @param timeOut
	// @return []byte
	// @return error
	ReadFileSection(fiIndex *storePb.StoreInfo, timeOut time.Duration) ([]byte, error)

	// DropOpenedFileCache drop opened file handle cache
	// @Description:
	// @param path
	// @return bool
	DropOpenedFileCache(path string) bool

	// CheckFileExist check file exist
	// @Description:
	// @param fiIndex
	// @return bool
	// @return error
	CheckFileExist(fiIndex *storePb.StoreInfo) (bool, error)

	// Close add next time
	//  @Description:
	//  @return error
	Close() error
}
