/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"encoding/json"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
)

// ResultSuccess make *common.ContractResult with data
func ResultSuccess(result []byte, gas uint64) *common.ContractResult {
	return &common.ContractResult{
		Code:          0,
		Result:        result,
		Message:       "OK",
		GasUsed:       gas,
		ContractEvent: nil,
	}
}

// ResultError make *common.ContractResult with err
func ResultError(err error) *common.ContractResult {
	return &common.ContractResult{
		Code:          1,
		Result:        nil,
		Message:       err.Error(),
		GasUsed:       0,
		ContractEvent: nil,
	}
}

// ResultBytesAndError make *common.ContractResult with data, event, err
func ResultBytesAndError(returnResult []byte, event []*common.ContractEvent, err error) *common.ContractResult {
	if err != nil {
		return &common.ContractResult{
			Code:          1,
			Result:        returnResult,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}
	//if len(returnResult) == 0 {
	//	returnResult = []byte("ok")
	//}
	return &common.ContractResult{
		Code:          0,
		Result:        returnResult,
		Message:       "OK",
		GasUsed:       0,
		ContractEvent: event,
	}
}

// ResultErrorWithGasUsed  构造一个包含Error结果的ContractResult对象
// @param err
// @param gas
// @return *common.ContractResult
func ResultErrorWithGasUsed(err error, gas uint64) *common.ContractResult {
	return &common.ContractResult{
		Code:          1,
		Result:        nil,
		Message:       err.Error(),
		GasUsed:       0,
		ContractEvent: nil,
	}
}

// ResultJson 构造一个包含输入对象Json作为Result的ContractResult对象
// @param obj
// @return *common.ContractResult
// @return error
func ResultJson(obj interface{}) (*common.ContractResult, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return &common.ContractResult{
		Code:          0,
		Result:        data,
		Message:       "OK",
		GasUsed:       0,
		ContractEvent: nil,
	}, nil
}

// WrapResultFunc 包装原有的返回[]byte的函数，改为返回ContractResult的函数
func WrapResultFunc(f func(txSimContext protocol.TxSimContext, parameters map[string][]byte) ([]byte, error)) func(
	txSimContext protocol.TxSimContext, parameters map[string][]byte) *common.ContractResult {
	f2 := func(txSimContext protocol.TxSimContext, parameters map[string][]byte) *common.ContractResult {
		result, err := f(txSimContext, parameters)
		return ResultBytesAndError(result, nil, err)
	}
	return f2
}

// WrapEventResult 包装原有的返回[]byte的函数，改为返回ContractResult和事件的函数，特别注意：若改为调用此方法则需要做兼容
func WrapEventResult(f func(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, []*common.ContractEvent, error)) func(
	txSimContext protocol.TxSimContext, parameters map[string][]byte) *common.ContractResult {
	f2 := func(txSimContext protocol.TxSimContext, parameters map[string][]byte) *common.ContractResult {
		configBytes, events, err := f(txSimContext, parameters)
		contractResult := ResultBytesAndError(configBytes, events, err)
		return contractResult
	}
	return f2
}
