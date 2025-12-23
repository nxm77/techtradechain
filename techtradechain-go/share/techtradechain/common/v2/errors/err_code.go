/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package errors

import "fmt"

// ErrCode 错误代码
type ErrCode int32

// 预定义的错误代码枚举
const (
	ERR_CODE_OK                                           ErrCode = iota
	ERR_CODE_SYSTEM_CONTRACT_PB_UNMARSHAL                 ErrCode = 1000 + iota
	ERR_CODE_SYSTEM_CONTRACT_UNKNOWN_TX_ROUTE_MAP         ErrCode = 1000 + iota
	ERR_CODE_SYSTEM_CONTRACT_UNSUPPORT_CONTRACT_NAME      ErrCode = 1000 + iota
	ERR_CODE_SYSTEM_CONTRACT_UNSUPPORT_METHOD_NAME        ErrCode = 1000 + iota
	ERR_CODE_SYSTEM_CONTRACT_QUERY_FAILED                 ErrCode = 1000 + iota
	ERR_CODE_SYSTEM_CONTRACT_CONTRACT_FAILED              ErrCode = 1000 + iota
	ERR_CODE_CHECK_PAYLOAD_PARAM_SUBSCRIBE_BLOCK          ErrCode = 1000 + iota
	ERR_CODE_CHECK_PAYLOAD_PARAM_SUBSCRIBE_TX             ErrCode = 1000 + iota
	ERR_CODE_CHECK_PAYLOAD_PARAM_SUBSCRIBE_CONTRACT_EVENT ErrCode = 1000 + iota
	ERR_CODE_CHECK_PAYLOAD_PARAM_ARCHIVE_BLOCK            ErrCode = 1000 + iota
	ERR_CODE_TX_ADD_FAILED                                ErrCode = 1000 + iota
	ERR_CODE_TX_VERIFY_FAILED                             ErrCode = 1000 + iota
	ERR_CODE_GET_CHAIN_CONF                               ErrCode = 1000 + iota
	ERR_CODE_GET_BLOCKCHAIN                               ErrCode = 1000 + iota
	ERR_CODE_GET_STORE                                    ErrCode = 1000 + iota
	ERR_CODE_GET_LAST_BLOCK                               ErrCode = 1000 + iota
	ERR_CODE_GET_VM_MGR                                   ErrCode = 1000 + iota
	ERR_CODE_GET_SUBSCRIBER                               ErrCode = 1000 + iota
	ERR_CODE_INVOKE_CONTRACT                              ErrCode = 1000 + iota
	ERR_CODE_TXTYPE                                       ErrCode = 1000 + iota
)

// ErrCodeName 错误代码对应的错误消息中英文字符串，value是2个字符串，0是英文，1是中文
var ErrCodeName = map[ErrCode][]string{
	ERR_CODE_OK:                                      {"OK", "OK"},
	ERR_CODE_SYSTEM_CONTRACT_PB_UNMARSHAL:            {"pb unmarshal failed", "系统合约PB结构解析失败"},
	ERR_CODE_SYSTEM_CONTRACT_UNKNOWN_TX_ROUTE_MAP:    {"unknown tx type route", "未知交易类型路由"},
	ERR_CODE_SYSTEM_CONTRACT_UNSUPPORT_CONTRACT_NAME: {"unsupport contract name", "不支持的合约名"},
	ERR_CODE_SYSTEM_CONTRACT_UNSUPPORT_METHOD_NAME:   {"unsupport method name", "不支持的方法名"},
	ERR_CODE_SYSTEM_CONTRACT_QUERY_FAILED:            {"call query contract failed", "调用查询系统合约失败"},
	ERR_CODE_SYSTEM_CONTRACT_CONTRACT_FAILED: {
		"call contract contract failed",
		"调用交易系统合约失败",
	},
	ERR_CODE_CHECK_PAYLOAD_PARAM_SUBSCRIBE_BLOCK: {
		"check subscribe block payload failed",
		"校验订阅区块Payload数据失败",
	},
	ERR_CODE_CHECK_PAYLOAD_PARAM_SUBSCRIBE_TX: {
		"check subscribe tx payload params failed",
		"校验订阅交易Payload数据失败",
	},
	ERR_CODE_CHECK_PAYLOAD_PARAM_SUBSCRIBE_CONTRACT_EVENT: {
		"check subscribe contract event payload params failed",
		"校验订阅合约事件Payload数据失败",
	},
	ERR_CODE_CHECK_PAYLOAD_PARAM_ARCHIVE_BLOCK: {
		"check archive block payload params failed",
		"校验数据归档数据失败",
	},
	ERR_CODE_TX_ADD_FAILED:    {"tx add failed", "添加交易失败"},
	ERR_CODE_TX_VERIFY_FAILED: {"tx verify failed", "验证交易失败"},
	ERR_CODE_GET_CHAIN_CONF:   {"get chain conf failed", "获取ChainConf对象失败"},
	ERR_CODE_GET_BLOCKCHAIN:   {"get blockchain failed", "获取Blockchain对象失败"},
	ERR_CODE_GET_STORE:        {"get store failed", "获取store对象失败"},
	ERR_CODE_GET_LAST_BLOCK:   {"get last block failed", "获取最后区块失败失败"},
	ERR_CODE_GET_SUBSCRIBER:   {"get subscriber failed", "获取subscriber对象失败"},
	ERR_CODE_GET_VM_MGR:       {"get vm manager failed", "获取VM Manager失败"},
	ERR_CODE_INVOKE_CONTRACT:  {"invoke contract failed", "VM虚拟机合约失败"},
	ERR_CODE_TXTYPE:           {"unsupport tx_type", "txType不支持"},
}

// String 根据错误代码获得对应的字符串
// @return string
func (e ErrCode) String() string {
	if s, ok := ErrCodeName[e]; ok {
		return s[0]
	}
	return fmt.Sprintf("unknown error code %d", e)
}

// Int 返回错误代码的int类型
// @return int32
func (e ErrCode) Int() int32 {
	return int32(e)
}
