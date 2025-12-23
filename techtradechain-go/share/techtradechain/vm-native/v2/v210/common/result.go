/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common210

import (
	"encoding/json"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
)

// ResultSuccess comment at next version
func ResultSuccess(result []byte, gas uint64) *common.ContractResult {
	return &common.ContractResult{
		Code:          0,
		Result:        result,
		Message:       "OK",
		GasUsed:       gas,
		ContractEvent: nil,
	}
}

// ResultError comment at next version
func ResultError(err error) *common.ContractResult {
	return &common.ContractResult{
		Code:          1,
		Result:        nil,
		Message:       err.Error(),
		GasUsed:       0,
		ContractEvent: nil,
	}
}

// ResultBytesAndError comment at next version
func ResultBytesAndError(returnResult []byte, err error) *common.ContractResult {
	if err != nil {
		return &common.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       err.Error(),
			GasUsed:       0,
			ContractEvent: nil,
		}
	}
	return &common.ContractResult{
		Code:          0,
		Result:        returnResult,
		Message:       "OK",
		GasUsed:       0,
		ContractEvent: nil,
	}
}

// ResultErrorWithGasUsed comment at next version
func ResultErrorWithGasUsed(err error, gas uint64) *common.ContractResult {
	return &common.ContractResult{
		Code:          1,
		Result:        nil,
		Message:       err.Error(),
		GasUsed:       0,
		ContractEvent: nil,
	}
}

// ResultJson comment at next version
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

//WrapResultFunc 包装原有的返回[]byte的函数，改为返回ContractResult的函数
func WrapResultFunc(f func(txSimContext protocol.TxSimContext, parameters map[string][]byte) ([]byte, error)) func(
	txSimContext protocol.TxSimContext, parameters map[string][]byte) *common.ContractResult {
	f2 := func(txSimContext protocol.TxSimContext, parameters map[string][]byte) *common.ContractResult {
		result, err := f(txSimContext, parameters)
		return ResultBytesAndError(result, err)
	}
	return f2
}
