// Package resulthelper package
package resulthelper

/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0

*/

import (
	"encoding/binary"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/store/v2/cache"
	"techtradechain.com/techtradechain/store/v2/serialization"
	"techtradechain.com/techtradechain/store/v2/types"
	"github.com/gogo/protobuf/proto"
)

// nolint
const (
	TxRWSetIdxKeyPrefix   = 'r'
	TxRWSetIndexKeyPrefix = "ri"
	ResultDBSavepointKey  = "resultSavepointKey"
)

// BuildKVBatch add next time
// @Description:
// @param batch
// @param blockInfo
// @param dbType
// @param logger
// @return *types.UpdateBatch
func BuildKVBatch(batch *types.UpdateBatch, blockInfo *serialization.BlockWithSerializedInfo, logger protocol.Logger) {
	if batch == nil {
		return
	}

	block := blockInfo.Block
	lastBlockNumBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lastBlockNumBytes, block.Header.BlockHeight)
	batch.Put([]byte(ResultDBSavepointKey), lastBlockNumBytes)

	txRWSets := blockInfo.TxRWSets
	rwsetData := blockInfo.SerializedTxRWSets
	for index, txRWSet := range txRWSets {
		// 6. rwset: txID -> txRWSet
		txRWSetBytes := rwsetData[index]
		txRWSetKey := ConstructTxRWSetIDKey(txRWSet.TxId)
		batch.Put(txRWSetKey, txRWSetBytes)
	}
}

// GetTxRWSetFromKVDB add next time
// @Description:
// @param txId
// @param dbHandle
// @param cache
// @return *commonPb.TxRWSet
// @return error
func GetTxRWSetFromKVDB(txId string, dbHandle protocol.DBHandle,
	cache *cache.StoreCacheMgr) (*commonPb.TxRWSet, error) {
	txRWSetKey := ConstructTxRWSetIDKey(txId)
	//get from cache
	var err error
	value, _ := cache.Get(string(txRWSetKey))
	if value == nil {
		value, err = dbHandle.Get(txRWSetKey)
	}
	if err != nil {
		return nil, err
	} else if value == nil {
		return nil, nil
	}

	var txRWSet commonPb.TxRWSet
	err = proto.Unmarshal(value, &txRWSet)
	if err != nil {
		return nil, err
	}
	return &txRWSet, nil
}

// ConstructTxRWSetIDKey return byte array
// format []byte{'r',txId}
// @Description:
// @param txId
// @return []byte
func ConstructTxRWSetIDKey(txId string) []byte {
	return append([]byte{TxRWSetIdxKeyPrefix}, txId...)
}

// ConstructTxRWSetIndexKey construct key
// format []byte{'r','i',txId}
// @Description:
// @param txId
// @return []byte
func ConstructTxRWSetIndexKey(txId string) []byte {
	key := append([]byte{}, TxRWSetIndexKeyPrefix...)
	return append(key, txId...)
}
