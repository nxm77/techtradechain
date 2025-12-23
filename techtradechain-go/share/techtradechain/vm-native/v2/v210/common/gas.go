/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common210

import "techtradechain.com/techtradechain/pb-go/v2/syscontract"

// DefaultGas comment at next version
const DefaultGas = uint64(100000)

// contractName: [functionName:gas]
var gasTable = map[string]map[string]uint64{
	syscontract.SystemContract_CHAIN_QUERY.String(): {"": DefaultGas}, //key: ""表示默认方法
	syscontract.SystemContract_CONTRACT_MANAGE.String(): {
		syscontract.ContractManageFunction_INIT_CONTRACT.String():    1000000,
		syscontract.ContractManageFunction_UPGRADE_CONTRACT.String(): 1000000,
		"": DefaultGas,
	},
}

// GetGas comment at next version
func GetGas(contractName, method string, defaultGas uint64) uint64 {
	methodGasMap, ok := gasTable[contractName]
	if !ok { //找不到合约的Gas设置
		return defaultGas
	}
	gas, ok2 := methodGasMap[method]
	if !ok2 { //找不到匹配的，就找key为空的
		contractGas, ok3 := methodGasMap[""]
		if !ok3 {
			return defaultGas
		}
		return contractGas
	}
	return gas
}
