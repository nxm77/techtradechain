/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package wasm

import (
	"techtradechain.com/techtradechain/protocol/v2"
)

const (
	blockVersion232 = uint32(2030200)
)

// SubtractGasForLogMessage charge gas for syscall of `LogMessage`
func SubtractGasForLogMessage(params []byte, txSimContext protocol.TxSimContext) error {
	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForLogMessage232(params, txSimContext)
}

// SubtractGasForSuccessResult charge gas for syscall of `SuccessResult`
func SubtractGasForSuccessResult(params []byte, txSimContext protocol.TxSimContext) error {
	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForSuccessResult232(params, txSimContext)
}

// SubtractGasForErrorResult charge gas for syscall of `ErrorResult`
func SubtractGasForErrorResult(params []byte, txSimContext protocol.TxSimContext) error {
	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForErrorResult232(params, txSimContext)
}

// SubtractGasForPutState charge gas for syscall of `PutState`
func SubtractGasForPutState(
	contractName string,
	stateKey []byte,
	value []byte,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForPutState232(
		contractName, stateKey, value, txSimContext)
}

// SubtractGasForGetState charge gas for syscall of `GetState`
func SubtractGasForGetState(
	contractName string, stateKey []byte, value []byte, txSimContext protocol.TxSimContext) error {
	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForGetState232(contractName, stateKey, value, txSimContext)
}

// SubtractGasForDeleteState charge gas for syscall of `DeleteState`
func SubtractGasForDeleteState(contractName string, stateKey []byte, txSimContext protocol.TxSimContext) error {
	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForDeleteState232(contractName, stateKey, txSimContext)
}

// SubtractGasForCallContract charge gas for syscall of `CallContract`
func SubtractGasForCallContract(
	contractName string, method string,
	params map[string][]byte, returns []byte, txSimContext protocol.TxSimContext) error {
	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForCallContract232(contractName, method, params, returns, txSimContext)
}

// SubtractGasForEmitEvent charge gas for syscall of `EmitEvent`
func SubtractGasForEmitEvent(topic string, events []string, txSimContext protocol.TxSimContext) error {
	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForEmitEvent232(topic, events, txSimContext)
}

// SubtractGasForKvIterator charge gas for syscall of `kv iterator Create`
func SubtractGasForKvIterator(
	key []byte,
	limit []byte,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForKvIterator232(key, limit, txSimContext)
}

//func SubtractGasForKvPreIterator(
//	key string,
//	limit string,
//	txSimContext protocol.TxSimContext) error {
//
//	blockVersion := txSimContext.GetBlockVersion()
//	if blockVersion < blockVersion232 {
//		return nil
//	}
//
//	return subtractGasForKvPreIterator232(key, limit, txSimContext)
//}

// SubtractGasForKvIteratorHasNext charge gas for syscall of `kv iterator HasNext`
func SubtractGasForKvIteratorHasNext(
	kvIndex int32,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForKvIteratorHasNext232(kvIndex, txSimContext)
}

// SubtractGasForKvIteratorNext charge gas for syscall of `kv iterator Next`
func SubtractGasForKvIteratorNext(
	key string, field string, value []byte,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForKvIteratorNext232(key, field, value, txSimContext)
}

// SubtractGasForKvIteratorClose charge gas for syscall of `kv iterator Close`
func SubtractGasForKvIteratorClose(
	kvIndex int32,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForKvIteratorClose232(kvIndex, txSimContext)
}

// SubtractGasForExecuteQuery charge gas for syscall of `ExecuteQuery`
func SubtractGasForExecuteQuery(
	sql string,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForExecuteQuery232(sql, txSimContext)
}

// SubtractGasForExecuteQueryOne charge gas for syscall of `ExecuteQueryOne`
func SubtractGasForExecuteQueryOne(
	sql string,
	returnData []byte,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForExecuteQueryOne232(sql, returnData, txSimContext)
}

// SubtractGasForRSHasNext charge gas for syscall of `rs HasNext`
func SubtractGasForRSHasNext(
	rsIndex int32,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForRSHasNext232(rsIndex, txSimContext)
}

// SubtractGasForRSNext charge gas for syscall of `rs Next`
func SubtractGasForRSNext(
	rsIndex int32,
	returnData []byte,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForRSNext232(rsIndex, returnData, txSimContext)
}

// SubtractGasForRSClose charge gas for syscall of `rs Close`
func SubtractGasForRSClose(
	rsIndex int32,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForRSClose232(rsIndex, txSimContext)
}

// SubtractGasForExecuteUpdate charge gas for syscall of `ExecuteUpdate`
func SubtractGasForExecuteUpdate(
	sql string,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForExecuteUpdate232(sql, txSimContext)
}

// SubtractGasForExecuteDDL charge gas for syscall of `ExecuteDDL`
func SubtractGasForExecuteDDL(
	sql string,
	txSimContext protocol.TxSimContext) error {

	blockVersion := txSimContext.GetBlockVersion()
	if blockVersion < blockVersion232 {
		return nil
	}

	return subtractGasForExecuteDDL232(sql, txSimContext)
}
