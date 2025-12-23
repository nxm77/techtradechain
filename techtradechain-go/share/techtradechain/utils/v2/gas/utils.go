/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package gas

import (
	"fmt"
	"math"
	"math/big"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"github.com/shopspring/decimal"
)

var (
	maxUint64 decimal.Decimal
)

func init() {
	bigInt := big.NewInt(0)
	bigInt.SetUint64(math.MaxUint64)
	maxUint64 = decimal.NewFromBigInt(bigInt, 0)
}

// GetGasLimit get `gasLimit` field from tx
func GetGasLimit(tx *common.Transaction) uint64 {
	if tx.Payload.Limit == nil {
		return 0
	}

	return tx.Payload.Limit.GasLimit
}

// IsGasEnabled check tha `enable_gas` flag
func IsGasEnabled(txSimContext protocol.TxSimContext) bool {
	lastChainConfig := txSimContext.GetLastChainConfig()
	if lastChainConfig != nil && lastChainConfig.AccountConfig != nil {
		return lastChainConfig.AccountConfig.EnableGas
	}

	return false
}

// MultiplyGasPrice calculate multiplication of contract size(int) and gas price(float32)
func MultiplyGasPrice(dataSize int, gasPrice float32) (uint64, error) {
	if gasPrice == 0 || dataSize == 0 {
		return uint64(0), nil
	}

	price := decimal.NewFromFloat32(gasPrice)
	size := decimal.NewFromInt(int64(dataSize))
	gas := size.Mul(price).Ceil()

	if gas.GreaterThan(maxUint64) {
		return 0, fmt.Errorf("uint64 `%v` overflow", gas)
	}

	return gas.BigInt().Uint64(), nil
}
