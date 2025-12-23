/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
)

// ContractFunc invoke contract method, return result
type ContractFunc func(context protocol.TxSimContext, params map[string][]byte) *common.ContractResult

// Contract define native Contract interface
// Deprecated: be deprecated in the future, please use ContractVersioned.
type Contract interface {
	// GetMethod get register method by name
	GetMethod(methodName string) ContractFunc
}

// ContractVersioned define native Contract interface with block version.
// You can specify in what version the contract method can be seen.
type ContractVersioned interface {
	// GetMethod get register method by name and block version.
	GetMethod(methodName string, version uint32) ContractFunc
}
