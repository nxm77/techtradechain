/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package errors

import (
	"fmt"

	"techtradechain.com/techtradechain/common/v2/json"
)

var (
	// json
	// -32768 to -32000 is the reserved predefined error code
	// the server received an invalid JSON. The error is sent to the server trying to parse the JSON text
	ErrParseError = JsonError{Code: -32700, Message: "Parse error"}
	// the sent JSON is not a valid request object
	ErrInvalidRequest = JsonError{Code: -32600, Message: "Invalid request"}
	// the method does not exist or is invalid
	ErrMethodNotFound = JsonError{Code: -32601, Message: "Method not found"}
	ErrInvalidParams  = JsonError{Code: -32602, Message: "Invalid params"} // invalid method parameter
	ErrInternalError  = JsonError{Code: -32603, Message: "Internal error"} // json-rpc internal error.
	// -32000 to -32099	is the server error reserved for customization

	// txPool
	// the object is nil
	ErrStructEmpty = JsonError{Code: -31100, Message: "Struct is nil"}
	// tx-id already exists
	ErrTxIdExist = JsonError{Code: -31105, Message: "TxId exist"}
	// tx-id already exists in DB
	ErrTxIdExistDB = JsonError{Code: -31106, Message: "TxId exist in DB"}
	// tx-timestamp out of range
	ErrTxTimeout = JsonError{Code: -31108, Message: "TxTimestamp error"}
	// transaction pool is full
	ErrTxPoolLimit = JsonError{Code: -31110, Message: "TxPool is full"}
	// tx-source is error
	ErrTxSource = JsonError{Code: -31112, Message: "TxSource is err"}
	// The tx had been on the block chain
	ErrTxHadOnTheChain = JsonError{Code: -31113, Message: "The tx had been on the block chain"}
	// The txPool service has stopped
	ErrTxPoolHasStopped = JsonError{Code: -31114, Message: "The tx pool has stopped"}
	// The txPool service has started
	ErrTxPoolHasStarted = JsonError{Code: -31115, Message: "The tx pool has started"}
	// The txPool service stop failed
	ErrTxPoolStopFailed = JsonError{Code: -31116, Message: "The tx pool stop failed"}
	// The txPool service start failed
	ErrTxPoolStartFailed = JsonError{Code: -31117, Message: "The tx pool start failed"}

	// core
	// block had been committed
	ErrBlockHadBeenCommited = JsonError{Code: -31200, Message: "Block had been committed err"}
	// block concurrent verify error
	ErrConcurrentVerify = JsonError{Code: -31201, Message: "Block concurrent verify err"}
	// block had been verified
	ErrRepeatedVerify = JsonError{Code: -31202, Message: "Block had been verified err"}

	// sync
	// The sync service has been started
	ErrSyncServiceHasStarted = JsonError{Code: -33000, Message: "The sync service has been started"}
	// The sync service has been stoped
	ErrSyncServiceHasStoped = JsonError{Code: -33001, Message: "The sync service has been stoped"}

	// store
	// The store service has been started
	ErrStoreServiceNeedRestarted = JsonError{Code: -34000, Message: "The store service need restart"}
)

// core
var (
	// WarnRwSetVerifyFailTxs Block rw set verify fail txs
	WarnRwSetVerifyFailTxs = JsonError{Code: -31203, Message: "Block rw set verify fail txs"}
)

// JsonError Json返回的错误
type JsonError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// String 获得错误的字符串
// @return string
func (err JsonError) String() string {
	marshal, _ := json.Marshal(err)
	return string(marshal)
}

// Error 获得错误的Message
// @return string
func (err JsonError) Error() string {
	if err.Message == "" {
		return fmt.Sprintf("error %d", err.Code)
	}
	return err.Message
}

// ErrorCode 获得错误的Code
// @return int
func (err JsonError) ErrorCode() int {
	return err.Code
}
