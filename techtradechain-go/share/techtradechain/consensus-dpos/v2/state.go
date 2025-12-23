/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dpos

import (
	"bytes"

	"techtradechain.com/techtradechain/pb-go/v2/common"
)

// getState Query value according to key in dpos contract
func (impl *DPoSImpl) getState(
	contractName string, key []byte, block *common.Block, blockTxRwSet map[string]*common.TxRWSet) ([]byte, error) {

	// Querying from a read-write set
	if len(block.Txs) > 0 {
		for i := len(block.Txs) - 1; i >= 0; i-- {
			rwSets := blockTxRwSet[block.Txs[i].Payload.TxId]
			for _, txWrite := range rwSets.TxWrites {
				if txWrite.ContractName == contractName && bytes.Equal(txWrite.Key, key) {
					return txWrite.Value, nil
				}
			}
		}
	}

	// Does not exist in the read-write set, query from the ledger
	val, err := impl.stateDB.ReadObject(contractName, key)
	if err != nil {
		impl.log.Errorf("query user balance failed, reason: %s", err)
		return nil, err
	}
	return val, nil
}
