/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

// common related definition
const (
	PUT_STATE_SYSCALL = "PutState"
	PUT_STATE_PARAMS  = "PutState_Params"
	PUT_STATE_RETURNS = "PutState_Returns"

	GET_STATE_SYSCALL = "GetState"
	GET_STATE_PARAMS  = "GetState_Params"
	GET_STATE_RETURNS = "GetState_Returns"

	DELETE_STATE_SYSCALL = "DeleteState"
	DELETE_STATE_PARAMS  = "DeleteState_Params"
	DELETE_STATE_RETURNS = "DeleteState_Returns"

	CALL_CONTRACT_SYSCALL = "CallContract"
	CALL_CONTRACT_PARAMS  = "CallContract_Params"
	CALL_CONTRACT_RETURNS = "CallContract_Returns"

	EMIT_EVENT_SYSCALL = "EmitEvent"
	EMIT_EVENT_PARAMS  = "EmitEvent_Params"
	EMIT_EVENT_RETURNS = "EmitEvent_Returns"
)

// wasm related definition
const (
	LOG_MESSAGE_SYSCALL = "LogMessage"
	LOG_MESSAGE_PARAMS  = "LogMessage_Params"
	LOG_MESSAGE_RETURNS = "LogMessage_Returns"

	SUCCESS_RESULT_SYSCALL = "SuccessResult"
	SUCCESS_RESULT_PARAMS  = "SuccessResult_Params"
	SUCCESS_RESULT_RETURNS = "SuccessResult_Returns"

	ERROR_RESULT_SYSCALL = "ErrorResult"
	ERROR_RESULT_PARAMS  = "ErrorResult_Params"
	ERROR_RESULT_RETURNS = "ErrorResult_Returns"
)

// docker related definition
const (
	GET_KEYS_SYSCALL = "GetKeys"
	GET_KEYS_PARAMS  = "GetKeys_Params"
	GET_KEYS_RETURNS = "GetKeys_Returns"
)

// iterator related definition
const (
	KV_ITER_SYSCALL = "KvIterator"
	KV_ITER_PARAMS  = "KvIterator_Params"
	KV_ITER_RETURNS = "KvIterator_Returns"

	KV_ITER_HAS_NEXT_SYSCALL = "KvIteratorHasNext"
	KV_ITER_HAS_NEXT_PARAMS  = "KvIteratorHasNext_Params"
	KV_ITER_HAS_NEXT_RETURNS = "KvIteratorHasNext_Returns"

	KV_ITER_NEXT_SYSCALL = "KvIteratorNext"
	KV_ITER_NEXT_PARAMS  = "KvIteratorNext_Params"
	KV_ITER_NEXT_RETURNS = "KvIteratorNext_Returns"

	KV_ITER_CLOSE_SYSCALL = "KvIteratorClose"
	KV_ITER_CLOSE_PARAMS  = "KvIteratorClose_Params"
	KV_ITER_CLOSE_RETURNS = "KvIteratorClose_Returns"

	KEY_HISTORY_ITER_SYSCALL = "KeyHistoryIter"
	KEY_HISTORY_ITER_PARAMS  = "KeyHistoryIter_Params"
	KEY_HISTORY_ITER_RETURNS = "KeyHistoryIter_Returns"

	KEY_HISTORY_ITER_HAS_NEXT_SYSCALL = "KeyHistoryIterHasNext"
	KEY_HISTORY_ITER_HAS_NEXT_PARAMS  = "KeyHistoryIterHasNext_Params"
	KEY_HISTORY_ITER_HAS_NEXT_RETURNS = "KeyHistoryIterHasNext_Returns"

	KEY_HISTORY_ITER_NEXT_SYSCALL = "KeyHistoryIterNext"
	KEY_HISTORY_ITER_NEXT_PARAMS  = "KeyHistoryIterNext_Params"
	KEY_HISTORY_ITER_NEXT_RETURNS = "KeyHistoryIterNext_Returns"

	KEY_HISTORY_ITER_CLOSE_SYSCALL = "KeyHistoryIterClose"
	KEY_HISTORY_ITER_CLOSE_PARAMS  = "KeyHistoryIterClose_Params"
	KEY_HISTORY_ITER_CLOSE_RETURNS = "KeyHistoryIterClose_Returns"
)

// sql related definition
const (
	EXEC_QUERY_SYSCALL = "ExecuteQuery"
	EXEC_QUERY_PARAMS  = "ExecuteQuery_Params"
	EXEC_QUERY_RETURNS = "ExecuteQuery_Returns"

	EXEC_QUERY_ONE_SYSCALL = "ExecuteQueryOne"
	EXEC_QUERY_ONE_PARAMS  = "ExecuteQueryOne_Params"
	EXEC_QUERY_ONE_RETURNS = "ExecuteQueryOne_Returns"

	RS_HAS_NEXT_SYSCALL = "RSHasNext"
	RS_HAS_NEXT_PARAMS  = "RSHasNext_Params"
	RS_HAS_NEXT_RETURNS = "RSHasNext_Returns"

	RS_NEXT_SYSCALL = "RSNext"
	RS_NEXT_PARAMS  = "RSNext_Params"
	RS_NEXT_RETURNS = "RSNext_Returns"

	RS_CLOSE_SYSCALL = "RSClose"
	RS_CLOSE_PARAMS  = "RSClose_Params"
	RS_CLOSE_RETURNS = "RSClose_Returns"

	EXEC_UPDATE_SYSCALL = "ExecuteUpdate"
	EXEC_UPDATE_PARAMS  = "ExecuteUpdate_Params"
	EXEC_UPDATE_RETURNS = "ExecuteUpdate_Returns"

	EXEC_DDL_SYSCALL = "ExecuteDDL"
	EXEC_DDL_PARAMS  = "ExecuteDDL_Params"
	EXEC_DDL_RETURNS = "ExecuteDDL_Returns"
)
