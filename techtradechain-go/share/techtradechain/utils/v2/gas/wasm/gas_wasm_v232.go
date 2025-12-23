/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package wasm

import (
	"fmt"
	"unsafe"

	"techtradechain.com/techtradechain/protocol/v2"

	commongas "techtradechain.com/techtradechain/utils/v2/gas/common"
)

var wasmDataGasPrice232 = map[string]uint64{
	commongas.LOG_MESSAGE_PARAMS:       uint64(1),
	commongas.LOG_MESSAGE_RETURNS:      uint64(1),
	commongas.SUCCESS_RESULT_PARAMS:    uint64(1),
	commongas.SUCCESS_RESULT_RETURNS:   uint64(1),
	commongas.ERROR_RESULT_PARAMS:      uint64(1),
	commongas.ERROR_RESULT_RETURNS:     uint64(1),
	commongas.PUT_STATE_PARAMS:         uint64(1),
	commongas.PUT_STATE_RETURNS:        uint64(1),
	commongas.GET_STATE_PARAMS:         uint64(1),
	commongas.GET_STATE_RETURNS:        uint64(1),
	commongas.DELETE_STATE_PARAMS:      uint64(1),
	commongas.DELETE_STATE_RETURNS:     uint64(1),
	commongas.CALL_CONTRACT_PARAMS:     uint64(1),
	commongas.CALL_CONTRACT_RETURNS:    uint64(1),
	commongas.EMIT_EVENT_PARAMS:        uint64(1),
	commongas.EMIT_EVENT_RETURNS:       uint64(1),
	commongas.KV_ITER_PARAMS:           uint64(1),
	commongas.KV_ITER_RETURNS:          uint64(1),
	commongas.KV_ITER_HAS_NEXT_PARAMS:  uint64(1),
	commongas.KV_ITER_HAS_NEXT_RETURNS: uint64(1),
	commongas.KV_ITER_NEXT_PARAMS:      uint64(1),
	commongas.KV_ITER_NEXT_RETURNS:     uint64(1),
	commongas.KV_ITER_CLOSE_PARAMS:     uint64(1),
	commongas.KV_ITER_CLOSE_RETURNS:    uint64(1),
	commongas.EXEC_QUERY_PARAMS:        uint64(1),
	commongas.EXEC_QUERY_RETURNS:       uint64(1),
	commongas.EXEC_QUERY_ONE_PARAMS:    uint64(1),
	commongas.EXEC_QUERY_ONE_RETURNS:   uint64(1),
	commongas.RS_HAS_NEXT_PARAMS:       uint64(1),
	commongas.RS_HAS_NEXT_RETURNS:      uint64(1),
	commongas.RS_NEXT_PARAMS:           uint64(1),
	commongas.RS_NEXT_RETURNS:          uint64(1),
	commongas.RS_CLOSE_PARAMS:          uint64(1),
	commongas.RS_CLOSE_RETURNS:         uint64(1),
	commongas.EXEC_UPDATE_PARAMS:       uint64(1),
	commongas.EXEC_UPDATE_RETURNS:      uint64(1),
	commongas.EXEC_DDL_PARAMS:          uint64(1),
	commongas.EXEC_DDL_RETURNS:         uint64(1),
}

var wasmSyscallGas232 = map[string]uint64{
	commongas.LOG_MESSAGE_SYSCALL:      uint64(1500),
	commongas.SUCCESS_RESULT_SYSCALL:   uint64(1000),
	commongas.ERROR_RESULT_SYSCALL:     uint64(1000),
	commongas.PUT_STATE_SYSCALL:        uint64(1000),
	commongas.GET_STATE_SYSCALL:        uint64(1000),
	commongas.DELETE_STATE_SYSCALL:     uint64(1000),
	commongas.CALL_CONTRACT_SYSCALL:    uint64(1000),
	commongas.EMIT_EVENT_SYSCALL:       uint64(1100),
	commongas.KV_ITER_SYSCALL:          uint64(10000),
	commongas.KV_ITER_HAS_NEXT_SYSCALL: uint64(2000),
	commongas.KV_ITER_NEXT_SYSCALL:     uint64(2100),
	commongas.KV_ITER_CLOSE_SYSCALL:    uint64(1200),
	commongas.EXEC_QUERY_SYSCALL:       uint64(10000),
	commongas.EXEC_QUERY_ONE_SYSCALL:   uint64(2000),
	commongas.RS_HAS_NEXT_SYSCALL:      uint64(2000),
	commongas.RS_NEXT_SYSCALL:          uint64(2100),
	commongas.RS_CLOSE_SYSCALL:         uint64(1200),
	commongas.EXEC_UPDATE_SYSCALL:      uint64(5000),
	commongas.EXEC_DDL_SYSCALL:         uint64(5000),
}

func calculateGasForWasmerSyscall232(
	txSimContext protocol.TxSimContext,
	syscallName string,
	paramLen int,
	gasPriceForParams uint64,
	returnLen int,
	gasPriceForReturns uint64) (uint64, error) {

	defaultGas, err := commongas.GetDefaultGas(txSimContext)
	if err != nil {
		return 0, err
	}

	syscallGas := uint64(0)
	syscallGas, ok := wasmSyscallGas232[syscallName]
	if !ok {
		return 0, fmt.Errorf("no such syscall `%s` for gas", syscallName)
	}

	return commongas.CalculateGas(
		defaultGas, syscallGas, gasPriceForParams, paramLen, gasPriceForReturns, returnLen)
}

func subtractGasForLogMessage232(params []byte, txSimContext protocol.TxSimContext) error {
	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.LOG_MESSAGE_PARAMS, commongas.LOG_MESSAGE_RETURNS)
	if err != nil {
		return err
	}

	gasUsed, err := calculateGasForWasmerSyscall232(txSimContext, commongas.LOG_MESSAGE_SYSCALL,
		len(params), gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForSuccessResult232(params []byte, txSimContext protocol.TxSimContext) error {
	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.SUCCESS_RESULT_PARAMS, commongas.SUCCESS_RESULT_RETURNS)
	if err != nil {
		return err
	}

	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.SUCCESS_RESULT_SYSCALL,
		len(params), gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForErrorResult232(params []byte, txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.ERROR_RESULT_PARAMS, commongas.ERROR_RESULT_RETURNS)
	if err != nil {
		return err
	}

	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.ERROR_RESULT_SYSCALL,
		len(params), gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForPutState232(
	contractName string,
	stateKey []byte,
	value []byte,
	txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.PUT_STATE_PARAMS, commongas.PUT_STATE_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(contractName) + len(stateKey) + len(value)
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.PUT_STATE_SYSCALL,
		paramsLen, gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForGetState232(
	contractName string, stateKey []byte, returnData []byte, txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.GET_STATE_PARAMS, commongas.GET_STATE_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(contractName) + len(stateKey)
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.GET_STATE_SYSCALL,
		paramsLen, gasPriceForParams, len(returnData), gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForDeleteState232(contractName string, stateKey []byte, txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.DELETE_STATE_PARAMS, commongas.DELETE_STATE_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(contractName) + len(stateKey)
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.DELETE_STATE_SYSCALL,
		paramsLen, gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForCallContract232(
	contractName string, method string,
	params map[string][]byte, returns []byte, txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.CALL_CONTRACT_PARAMS, commongas.CALL_CONTRACT_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(contractName) + len(method)
	for key, value := range params {
		paramsLen += len(key) + len(value)
	}
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.CALL_CONTRACT_SYSCALL,
		paramsLen, gasPriceForParams, len(returns), gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForEmitEvent232(topic string, events []string, txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.EMIT_EVENT_PARAMS, commongas.EMIT_EVENT_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(topic)
	for _, e := range events {
		paramsLen += len(e)
	}
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.EMIT_EVENT_SYSCALL,
		paramsLen, gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

//func subtractGasForPaillierOperation232(
//	opTypeStr string,
//	operandOne []byte,
//	operandTwo []byte,
//	pubKeyBytes []byte,
//	returnData []byte,
//	txSimContext protocol.TxSimContext) error {
//
//	gasPriceForParams, ok := wasmDataGasPrice232["PaillierOperationLen_Params"]
//	if !ok {
//		return fmt.Errorf("no gas price for PaillierOperationLen's param data")
//	}
//	gasPriceForReturns, ok := wasmDataGasPrice232["PaillierOperationLen_Returns"]
//	if !ok {
//		return fmt.Errorf("no gas price for PaillierOperationLen's return data")
//	}
//
//	paramLen := len(opTypeStr) + len(operandOne) + len(operandTwo) + len(pubKeyBytes)
//	gasUsed, err := calculateGasForWasmerSyscall232(
//		txSimContext, "PaillierOperationLen",
//		paramLen, gasPriceForParams, len(returnData), gasPriceForReturns)
//	if err != nil {
//		return err
//	}
//
//	return txSimContext.SubtractGas(gasUsed)
//}
//
//func subtractGasForBulletProofsOperation232(
//	opTypeStr string,
//	param1 []byte,
//	param2 []byte,
//	returnData []byte,
//	txSimContext protocol.TxSimContext) error {
//
//	gasPriceForParams, ok := wasmDataGasPrice232["BulletProofsOperationLen_Params"]
//	if !ok {
//		return fmt.Errorf("no gas price for BulletProofsOperationLen's param data")
//	}
//	gasPriceForReturns, ok := wasmDataGasPrice232["BulletProofsOperationLen_Returns"]
//	if !ok {
//		return fmt.Errorf("no gas price for BulletProofsOperationLen's return data")
//	}
//
//	paramsLen := len(opTypeStr) + len(param1) + len(param2)
//	gasUsed, err := calculateGasForWasmerSyscall232(
//		txSimContext, "BulletProofsOperationLen",
//		paramsLen, gasPriceForParams, len(returnData), gasPriceForReturns)
//	if err != nil {
//		return err
//	}
//
//	return txSimContext.SubtractGas(gasUsed)
//}

func subtractGasForKvIterator232(
	key []byte,
	limit []byte,
	txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.KV_ITER_PARAMS, commongas.KV_ITER_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(key) + len(limit)
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.KV_ITER_SYSCALL,
		paramsLen, gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForKvIteratorHasNext232(
	kvIndex int32,
	txSimContext protocol.TxSimContext) error {

	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.KV_ITER_HAS_NEXT_SYSCALL,
		0, 0, 0, 0)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForKvIteratorNext232(
	key string,
	field string,
	value []byte,
	txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.KV_ITER_NEXT_PARAMS, commongas.KV_ITER_NEXT_RETURNS)
	if err != nil {
		return err
	}

	returnLen := len(key) + len(field) + len(value)
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.KV_ITER_NEXT_SYSCALL,
		0, gasPriceForParams, returnLen, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForKvIteratorClose232(
	kvIndex int32,
	txSimContext protocol.TxSimContext) error {

	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.KV_ITER_CLOSE_SYSCALL,
		0, 0, 0, 0)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForExecuteQuery232(
	sql string,
	txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.EXEC_QUERY_PARAMS, commongas.EXEC_QUERY_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(sql)
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.EXEC_QUERY_SYSCALL,
		paramsLen, gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForExecuteQueryOne232(
	sql string,
	returnData []byte,
	txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.EXEC_QUERY_ONE_PARAMS, commongas.EXEC_QUERY_ONE_RETURNS)
	if err != nil {
		return err
	}

	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.EXEC_QUERY_ONE_SYSCALL,
		len(sql), gasPriceForParams, len(returnData), gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForRSHasNext232(
	rsIndex int32,
	txSimContext protocol.TxSimContext) error {

	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, "RSHasNext",
		0, 0, 0, 0)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForRSNext232(
	rsIndex int32,
	returnData []byte,
	txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.RS_NEXT_PARAMS, commongas.RS_NEXT_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := int(unsafe.Sizeof(rsIndex))
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.RS_NEXT_SYSCALL,
		paramsLen, gasPriceForParams, len(returnData), gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForRSClose232(
	rsIndex int32,
	txSimContext protocol.TxSimContext) error {

	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.RS_CLOSE_SYSCALL,
		0, 0, 0, 0)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForExecuteUpdate232(
	sql string,
	txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.EXEC_UPDATE_PARAMS, commongas.EXEC_UPDATE_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(sql)
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.EXEC_UPDATE_SYSCALL,
		paramsLen, gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func subtractGasForExecuteDDL232(
	sql string,
	txSimContext protocol.TxSimContext) error {

	gasPriceForParams, gasPriceForReturns, err :=
		getGasPrice(commongas.EXEC_DDL_PARAMS, commongas.EXEC_DDL_RETURNS)
	if err != nil {
		return err
	}

	paramsLen := len(sql)
	gasUsed, err := calculateGasForWasmerSyscall232(
		txSimContext, commongas.EXEC_DDL_SYSCALL,
		paramsLen, gasPriceForParams, 0, gasPriceForReturns)
	if err != nil {
		return err
	}

	return txSimContext.SubtractGas(gasUsed)
}

func getGasPrice(paramsType string, returnsType string) (uint64, uint64, error) {

	var ok bool
	var gasPriceForParams uint64
	var gasPriceForReturns uint64

	gasPriceForParams, ok = wasmDataGasPrice232[paramsType]
	if !ok {
		return 0, 0, fmt.Errorf("no gas price for %s's param data", paramsType)
	}
	gasPriceForReturns, ok = wasmDataGasPrice232[returnsType]
	if !ok {
		return 0, 0, fmt.Errorf("no gas price for %s's return data", returnsType)
	}

	return gasPriceForParams, gasPriceForReturns, nil
}
