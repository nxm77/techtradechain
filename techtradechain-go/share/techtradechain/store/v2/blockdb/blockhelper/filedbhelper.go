// Package blockhelper package
package blockhelper

/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0

*/
import (
	"fmt"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"github.com/gogo/protobuf/proto"
)

// nolint
const (
	BlockIndexKeyPrefix     = "ib"
	BlockMetaIndexKeyPrefix = "im"
)

// ConstructBlockMetaIndexKey build block meta index key
//
//	@Description:
//	@param dbType
//	@param blockNum
//	@return []byte
func ConstructBlockMetaIndexKey(dbType string, blockNum uint64) []byte {
	var blkNumBytes []byte
	if dbType == DbType_Mysql {
		blkNumBytes = []byte(fmt.Sprint(blockNum))
	} else {
		blkNumBytes = EncodeBlockNum(blockNum)
	}
	key := append([]byte{}, BlockMetaIndexKeyPrefix...)
	return append(key, blkNumBytes...)
}

// ConstructBlockIndexKey build block index key
// format: ib{blockNum}
//
//	@Description:
//	@param dbType
//	@param blockNum
//	@return []byte
func ConstructBlockIndexKey(dbType string, blockNum uint64) []byte {
	var blkNumBytes []byte
	if dbType == DbType_Mysql {
		blkNumBytes = []byte(fmt.Sprint(blockNum))
	} else {
		blkNumBytes = EncodeBlockNum(blockNum)
	}
	key := append([]byte{}, BlockIndexKeyPrefix...)
	return append(key, blkNumBytes...)
}

// ConstructFBTxIDBlockInfo marshal TransactionStoreInfo
//
//	@Description:
//	@param height
//	@param blockHash
//	@param txIndex
//	@param timestamp
//	@param blockFileIndex
//	@param txFileIndex
//	@return []byte
func ConstructFBTxIDBlockInfo(height uint64, blockHash []byte, txIndex uint32, timestamp int64,
	blockFileIndex, txFileIndex *storePb.StoreInfo) []byte {
	//value := fmt.Sprintf("%d,%x,%d", height, blockHash, txIndex)
	//return []byte(value)
	var transactionFileIndex *storePb.StoreInfo
	if txFileIndex != nil {
		transactionFileIndex = &storePb.StoreInfo{
			FileName: blockFileIndex.FileName,
			Offset:   blockFileIndex.Offset + txFileIndex.Offset,
			ByteLen:  txFileIndex.ByteLen,
		}
	}
	txInf := &storePb.TransactionStoreInfo{
		BlockHeight:          height,
		BlockHash:            nil, //for performance, set nil
		TxIndex:              txIndex,
		BlockTimestamp:       timestamp,
		TransactionStoreInfo: transactionFileIndex,
	}
	data, _ := txInf.Marshal()
	return data
}

// ParseFBTxIdBlockInfo retrieve TransactionStoreInfo
//
//	@Description:
//	@param value
//	@return height
//	@return blockHash
//	@return txIndex
//	@return timestamp
//	@return txFileIndex
//	@return err
func ParseFBTxIdBlockInfo(value []byte) (height uint64, blockHash []byte, txIndex uint32, timestamp int64,
	txFileIndex *storePb.StoreInfo, err error) {
	if len(value) == 0 {
		err = fmt.Errorf("input is empty,in parseTxIdBlockInfo")
		return
	}
	//新版本使用TransactionInfo，是因为经过BenchmarkTest速度会快很多
	var txInfo storePb.TransactionStoreInfo
	err = txInfo.Unmarshal(value)
	if err != nil {
		return
	}
	height = txInfo.BlockHeight
	blockHash = txInfo.BlockHash
	txIndex = txInfo.TxIndex
	timestamp = txInfo.BlockTimestamp
	txFileIndex = txInfo.TransactionStoreInfo
	err = nil
	return
}

// HandleFDKVTx retrieves a transaction by txId in file block archived config block's tx, or returns nil if none exists.
// this tx is moved to kvDB while archive before
//
//	@Description:
//	@receiver b
//	@return *commonPb.Transaction
//	@return error
func HandleFDKVTx(_ string, vBytes []byte, err error) (*commonPb.Transaction, error) {
	if err != nil {
		return nil, err
	} else if len(vBytes) == 0 {
		return nil, nil
	}

	var tx commonPb.Transaction
	err = proto.Unmarshal(vBytes, &tx)
	if err != nil {
		return nil, err
	}

	return &tx, nil
}

// HandleFDKVRWSet retrieves a transaction rwSet by txId in file block archived config block's tx,
// or returns nil if none exists. this rwSet is moved to kvDB while archive before
//
//	@Description:
//	@receiver b
//	@param txId
//	@return *commonPb.Transaction
//	@return error
func HandleFDKVRWSet(vBytes []byte, err error) (*commonPb.TxRWSet, error) {
	if err != nil {
		return nil, err
	} else if len(vBytes) == 0 {
		return nil, nil
	}

	var rw commonPb.TxRWSet
	err = proto.Unmarshal(vBytes, &rw)
	if err != nil {
		return nil, err
	}

	return &rw, nil
}
