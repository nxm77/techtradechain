/*
 * Copyright (C) BABEC. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package transactionmgr is package for transactionmgr
package transactionmgr

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/common/v2/msgbus"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
)

const (
	// Separator for kv iterator split
	Separator = "@"
	// KeyPrefix blacklist tx ids key prefix
	KeyPrefix = "blacklistTxIds" + Separator
)
const (
	paramNameBlackTxIdList = "txIds"
	paramNameBlackTxId     = "txId"
	inBlacklist            = "1"
	maxTxIdLen             = 64
	maxTxIdsLen            = 100
	optionAdd              = 0
	optionDel              = 1
)

// TransactionMgrContract 交易管理合约
type TransactionMgrContract struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewTransactionMgrContract TransactionMgrContract构造函数
// @param log
// @return *TransactionMgrContract
func NewTransactionMgrContract(log protocol.Logger) *TransactionMgrContract {
	return &TransactionMgrContract{
		log:     log,
		methods: registerTransactionMgrContractMethods(log),
	}
}

// GetMethod get register method by name
func (c *TransactionMgrContract) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

// TransactionMgr transaction blacklist management
type TransactionMgr struct {
	log protocol.Logger
}

// registerTransactionMgrContractMethods register contract methods
// @param log
// @map[string]common.ContractFunc
func registerTransactionMgrContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	q := make(map[string]common.ContractFunc, 64)

	t := &TransactionMgr{log: log}
	q[syscontract.TransactionManagerFunction_ADD_BLACKLIST_TX_IDS.String()] = t.AddBlacklistTxIds
	q[syscontract.TransactionManagerFunction_DELETE_BLACKLIST_TX_IDS.String()] = t.DeleteBlacklistTxIds
	q[syscontract.TransactionManagerFunction_GET_BLACKLIST_TX_IDS.String()] = t.GetBlacklistTxIds
	return q
}

// AddBlacklistTxIds add txIds to blacklist
// @param txIds 交易id，通过逗号分隔，如： "txId1,txId2,txId3..."
// @return "ok"
func (t *TransactionMgr) AddBlacklistTxIds(txSimContext protocol.TxSimContext,
	params map[string][]byte) (contractResult *commonPb.ContractResult) {
	return t.updateBlacklistTxIds(txSimContext, params, optionAdd)
}

// DeleteBlacklistTxIds delete txIds to blacklist
// @param txIds 交易id，通过逗号分隔，如： "txId1,txId2,txId3..."
// @return "ok"
func (t *TransactionMgr) DeleteBlacklistTxIds(txSimContext protocol.TxSimContext,
	params map[string][]byte) (contractResult *commonPb.ContractResult) {
	return t.updateBlacklistTxIds(txSimContext, params, optionDel)
}

// updateBlacklistTxIds delete or add txIds to blacklist
// @param txIds 交易id，通过逗号分隔，如： "txId1,txId2,txId3..."
// @param option 0 add 1 del
// @return "ok"
func (t *TransactionMgr) updateBlacklistTxIds(txSimContext protocol.TxSimContext,
	params map[string][]byte, option int) (contractResult *commonPb.ContractResult) {
	var (
		topic string
		err   error
	)
	contractResult = &commonPb.ContractResult{}
	if err = t.checkAdmin(txSimContext); err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		return
	}

	txIds := params[paramNameBlackTxIdList]
	if len(txIds) == 0 {
		contractResult.Code = 1
		contractResult.Message = "the key `txIds` is not found in params"
		return
	}
	ids := strings.Split(string(txIds), ",")
	if len(ids) > maxTxIdsLen {
		contractResult.Code = 1
		contractResult.Message = "the key `txIds` batch operation tx id exceeds 100"
		return
	}
	for i := 0; i < len(ids); i++ {
		if len(ids[i]) == 0 || len(ids[i]) > maxTxIdLen {
			contractResult.Code = 1
			contractResult.Message = "the txId length is invalid, must 1-64"
			return
		}
		if option == optionAdd {
			err = txSimContext.Put(syscontract.SystemContract_TRANSACTION_MANAGER.String(), []byte(KeyPrefix+ids[i]), []byte(inBlacklist))
		} else if option == optionDel {
			err = txSimContext.Del(syscontract.SystemContract_TRANSACTION_MANAGER.String(), []byte(KeyPrefix+ids[i]))
		}
		if err != nil {
			contractResult.Code = 1
			contractResult.Message = err.Error()
			return
		}
	}
	if option == optionAdd {
		topic = strconv.Itoa(int(msgbus.BlacklistTxIdAdd))
		t.log.Infof("receive add blacklist %s", txIds)
	} else if option == optionDel {
		topic = strconv.Itoa(int(msgbus.BlacklistTxIdDel))
		t.log.Infof("receive del blacklist %s", txIds)
	}

	eventData := append([]string{txSimContext.GetTx().GetPayload().GetChainId()}, ids...)
	event := &commonPb.ContractEvent{
		Topic:           topic,
		TxId:            txSimContext.GetTx().Payload.TxId,
		ContractName:    syscontract.SystemContract_TRANSACTION_MANAGER.String(),
		ContractVersion: "0",
		EventData:       eventData, // []string{"chainId", "txId1", "txId2"...}
	}
	contractResult.ContractEvent = []*commonPb.ContractEvent{event}
	contractResult.Code = 0
	contractResult.Result = []byte("ok")
	return
}

// GetBlacklistTxIds 查询交易是否存在黑名单中
// @param txIds 交易id
// @return "1" ：存在黑名单中，其他表示不存在
func (t *TransactionMgr) GetBlacklistTxIds(txSimContext protocol.TxSimContext,
	params map[string][]byte) (contractResult *commonPb.ContractResult) {
	contractResult = &commonPb.ContractResult{}

	txIds := params[paramNameBlackTxIdList]
	if len(txIds) == 0 {
		contractResult.Code = 1
		contractResult.Message = "the key `txIds` is not found in params"
		return
	}
	ids := strings.Split(string(txIds), ",")

	r := make([]string, 0)
	for i := 0; i < len(ids); i++ {
		v, err := txSimContext.Get(syscontract.SystemContract_TRANSACTION_MANAGER.String(), []byte(KeyPrefix+ids[i]))
		if err != nil {
			contractResult.Code = 1
			contractResult.Message = err.Error()
			return
		}
		r = append(r, string(v))
	}

	result, err := json.Marshal(r)
	if err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		return
	}

	contractResult.Code = 0
	contractResult.Result = result
	return
}

// checkAdmin check if the sender is an gas admin address
// @param txSimContext
// @return error address valid if nil
func (t *TransactionMgr) checkAdmin(txSimContext protocol.TxSimContext) error {
	userPublicKey, err := common.GetSenderPublicKey(txSimContext)
	if err != nil {
		return err
	}

	chainConfig := txSimContext.GetLastChainConfig()
	if chainConfig.AccountConfig == nil {
		return errors.New("gas admin address not set")
	}
	adminPublicKeyBytes := []byte(chainConfig.AccountConfig.GasAdminAddress)

	publicKeyString, err2 := common.PublicKeyToAddress(userPublicKey, chainConfig)
	if err2 != nil {
		return err2
	}

	if strings.EqualFold(string(adminPublicKeyBytes), publicKeyString) {
		return nil
	} else {
		return errors.New("sender not gas admin")
	}
}
