/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package native is package for native
package native

import (
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
)

// IsNative is a native contract
// @param contractName
// @param txType
// @return bool
func IsNative(contractName string, txType commonPb.TxType) bool {
	return IsNativeContract(contractName) && IsNativeTxType(txType)
}

// IsNativeContract return is native contract name
func IsNativeContract(contractName string) bool {
	_, ok := syscontract.SystemContract_value[contractName]
	return ok
}

// IsNativeTxType return is native contract supported transaction type
func IsNativeTxType(txType commonPb.TxType) bool {
	switch txType {
	case commonPb.TxType_QUERY_CONTRACT,
		commonPb.TxType_INVOKE_CONTRACT:
		//commonPb.TxType_INVOKE_CONTRACT:
		return true
	default:
		return false
	}
}
