/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package government is package for government
package government

import (
	"fmt"

	"techtradechain.com/techtradechain/vm-native/v2/common"

	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
)

const (
	// GovernmentContractName 治理合约名
	GovernmentContractName = "government_contract"
)

// GovernmentContract 治理合约
type GovernmentContract struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewGovernmentContract 新建治理合约对象
// @param log
// @return *GovernmentContract
func NewGovernmentContract(log protocol.Logger) *GovernmentContract {
	return &GovernmentContract{
		log:     log,
		methods: registerGovernmentContractMethods(log),
	}
}

// GetMethod get register method by name
func (c *GovernmentContract) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

func registerGovernmentContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	// cert manager
	governmentRuntime := &GovernmentRuntime{log: log}
	methodMap[syscontract.ChainQueryFunction_GET_GOVERNANCE_CONTRACT.String()] = common.WrapResultFunc(
		governmentRuntime.GetGovernmentContract)

	return methodMap
}

// GovernmentRuntime 治理合约方法的运行时
type GovernmentRuntime struct {
	log protocol.Logger
}

// GetGovernmentContract 获得治理合约对象
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *GovernmentRuntime) GetGovernmentContract(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	store := txSimContext.GetBlockchainStore()
	governmentContractName := GovernmentContractName
	bytes, err := store.ReadObject(governmentContractName, []byte(governmentContractName))
	if err != nil {
		r.log.Errorw("ReadObject.Get err", "governmentContractName", governmentContractName, "err", err)
		return nil, err
	}

	if len(bytes) == 0 {
		r.log.Errorw("ReadObject.Get empty", "governmentContractName", governmentContractName)
		return nil, fmt.Errorf("bytes is empty")
	}

	return bytes, nil
}
