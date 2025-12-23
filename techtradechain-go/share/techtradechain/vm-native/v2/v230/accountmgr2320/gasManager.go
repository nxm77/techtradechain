/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package accountmgr2320

import "techtradechain.com/techtradechain/protocol/v2"

// AccountMgr comment at next version
type AccountMgr interface {
	/**
	 * @Description: 设置gas管理员
	 * @param publicKey
	 * @return bool
	 * @return error
	 */
	SetAdmin(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 查询gas管理员
	 * @param publicKey
	 * @return []byte
	 * @return error
	 */
	GetAdmin(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 充值gas
	 * @param publicKey
	 * @param gas
	 * @return bool
	 * @return error
	 */
	RechargeGas(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 扣款for vm
	 * @param publicKey
	 * @param gas
	 * @return bool
	 * @return error
	 */
	ChargeGasVm(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 多用户扣款for vm
	 * @param publicKey
	 * @param gas
	 * @return bool
	 * @return error
	 */
	ChargeGasVmForMultiAccount(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 退还多扣的gas for vm
	 * @param: publicKey 发起交易的账户公钥
	 * @param: gas  退还gas数量
	 * @return bool  是否退还成功
	 * @return error
	 */
	RefundGasVm(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 查询gas余额
	 * @param publicKey
	 * @return uint64
	 * @return error
	 */
	GetBalance(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 退款for sdk
	 * @param publicKey
	 * @param gasUsed
	 * @return bool
	 * @return error
	 */
	RefundGas(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 冻结指定账户
	 * @param publicKey
	 * @return bool
	 * @return error
	 */
	FrozenAccount(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 解冻指定账号
	 * @param txSimContext
	 * @param params
	 * @return []byte
	 * @return error
	 */
	UnFrozenAccount(txSimContext protocol.TxSimContext, params map[string][]byte) ([]byte, error)

	/**
	 * @Description: 获取账户冻结状态
	 * @param publicKey
	 * @return bool
	 * @return error
	 */
	GetAccountStatus(context protocol.TxSimContext, params map[string][]byte) ([]byte, error)
}
