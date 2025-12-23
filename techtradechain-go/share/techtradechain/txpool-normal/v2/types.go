/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package normal

import (
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
)

type txValidateFunc func(tx *memTx) error

// transaction in tx pool
type memTx struct {
	tx       *commonPb.Transaction
	source   protocol.TxSource
	dbHeight uint64
}

// newMemTx create memTx
func newMemTx(tx *commonPb.Transaction, source protocol.TxSource, dbHeight uint64) *memTx {
	return &memTx{
		tx:       tx,
		source:   source,
		dbHeight: dbHeight,
	}
}

// getTx get tx in memTx
func (memTx *memTx) getTx() *commonPb.Transaction {
	return memTx.tx
}

// getChainId get chainId in tx
func (memTx *memTx) getChainId() string {
	return memTx.tx.Payload.ChainId
}

// getTxId get txId in tx
func (memTx *memTx) getTxId() string {
	return memTx.tx.Payload.TxId
}
