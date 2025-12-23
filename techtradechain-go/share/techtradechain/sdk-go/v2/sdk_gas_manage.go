/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/gogo/protobuf/proto"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
)

// CreateSetGasAdminPayload create set gas admin payload
func (cc *ChainClient) CreateSetGasAdminPayload(address string) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [CreateSetGasAdminPayload] payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyGasAddressKey,
			Value: []byte(address),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, err
	}

	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_SET_ACCOUNT_MANAGER_ADMIN.String(), pairs, seq+1, nil), nil
}

// GetGasAdmin get gas admin on chain, returns gas admin address
func (cc *ChainClient) GetGasAdmin() (string, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]",
		syscontract.GasAccountFunction_GET_ADMIN)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_GET_ADMIN.String(), nil, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return "", fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		return "", fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	return string(resp.ContractResult.Result), nil
}

// CreateRechargeGasPayload create recharge gas payload
func (cc *ChainClient) CreateRechargeGasPayload(rechargeGasList []*syscontract.RechargeGas) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [CreateRechargeGasPayload] payload")

	rechargeGasReq := syscontract.RechargeGasReq{BatchRechargeGas: rechargeGasList}
	bz, err := rechargeGasReq.Marshal()
	if err != nil {
		return nil, err
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyGasBatchRecharge,
			Value: bz,
		},
	}

	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_RECHARGE_GAS.String(), pairs, defaultSeq, nil), nil
}

// GetGasBalance returns gas balance of address
func (cc *ChainClient) GetGasBalance(address string) (int64, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]",
		syscontract.GasAccountFunction_GET_BALANCE)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_GET_BALANCE.String(), []*common.KeyValuePair{
			{
				Key:   utils.KeyGasAddressKey,
				Value: []byte(address),
			},
		}, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return 0, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err := utils.CheckProposalRequestResp(resp, true); err != nil {
		return 0, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	balance, err := strconv.ParseInt(string(resp.ContractResult.Result), 10, 64)
	if err != nil {
		return 0, fmt.Errorf(errStringFormat, "strconv.ParseInt", err)
	}

	return balance, nil
}

// CreateRefundGasPayload create refund gas payload
func (cc *ChainClient) CreateRefundGasPayload(address string, amount int64) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [CreateRefundGasPayload] payload")

	if amount <= 0 {
		return nil, errors.New("amount must > 0")
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyGasAddressKey,
			Value: []byte(address),
		},
		{
			Key:   utils.KeyGasChargeGasAmount,
			Value: []byte(strconv.FormatInt(amount, 10)),
		},
	}

	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_REFUND_GAS.String(), pairs, defaultSeq, nil), nil
}

// CreateFrozenGasAccountPayload create frozen gas account payload
func (cc *ChainClient) CreateFrozenGasAccountPayload(address string) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [CreateFrozenGasAccountPayload] payload")

	return cc.createFrozenUnfrozenGasAccountPayload(syscontract.GasAccountFunction_FROZEN_ACCOUNT.String(), address)
}

// CreateUnfrozenGasAccountPayload create unfrozen gas account payload
func (cc *ChainClient) CreateUnfrozenGasAccountPayload(address string) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [CreateFrozenGasAccountPayload] payload")

	return cc.createFrozenUnfrozenGasAccountPayload(syscontract.GasAccountFunction_UNFROZEN_ACCOUNT.String(), address)
}

// GetGasAccountStatus get gas account status
func (cc *ChainClient) GetGasAccountStatus(address string) (bool, error) {
	cc.logger.Debugf("[SDK] begin to QUERY system contract, [method:%s]",
		syscontract.GasAccountFunction_ACCOUNT_STATUS)

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyGasAddressKey,
			Value: []byte(address),
		},
	}

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		syscontract.GasAccountFunction_ACCOUNT_STATUS.String(), pairs, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, -1, false)
	if err != nil {
		return false, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	if err := utils.CheckProposalRequestResp(resp, true); err != nil {
		return false, fmt.Errorf(errStringFormat, payload.TxType, err)
	}

	return string(resp.ContractResult.Result) == "0", nil
}

// SendGasManageRequest send gas manage request to node
func (cc *ChainClient) SendGasManageRequest(payload *common.Payload, endorsers []*common.EndorsementEntry,
	timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	cc.logger.Debug("[SDK] begin SendGasManageRequest")
	return cc.proposalRequest(payload, endorsers, nil, timeout, withSyncResult)
}

func (cc *ChainClient) createFrozenUnfrozenGasAccountPayload(method string,
	address string) (*common.Payload, error) {

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeyGasAddressKey,
			Value: []byte(address),
		},
	}

	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_ACCOUNT_MANAGER.String(),
		method, pairs, defaultSeq, nil), nil
}

// AttachGasLimit attach gas limit for payload
func (cc *ChainClient) AttachGasLimit(payload *common.Payload, limit *common.Limit) *common.Payload {
	payload.Limit = limit
	return payload
}

// EstimateGas estimate gas used of payload
func (cc *ChainClient) EstimateGas(payload *common.Payload) (uint64, error) {
	cc.logger.Debugf("[SDK] begin EstimateGas")
	estimateGasPayload, err := cc.estimateGasPayload(payload)
	if err != nil {
		cc.logger.Errorf("create estimate gas payload failed: %s", err.Error())
		return 0, err
	}
	resp, err := cc.proposalRequest(estimateGasPayload, nil, nil, -1, false)
	if err != nil {
		return 0, err
	}

	if resp.Code != 0 {
		return 0, errors.New(resp.Message)
	}
	return resp.ContractResult.GasUsed, nil
}

// CreateSetInvokeBaseGasPayload create set invoke base gas payload
func (cc *ChainClient) CreateSetInvokeBaseGasPayload(amount int64) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] begin CreateSetInvokeBaseGasPayload")

	if amount < 0 {
		return nil, errors.New("amount must >= 0")
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeySetInvokeBaseGas,
			Value: []byte(strconv.FormatInt(amount, 10)),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, err
	}

	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_SET_INVOKE_BASE_GAS.String(), pairs, seq+1, nil), nil
}

// CreateSetInvokeGasPricePayload create set invoke gas price payload
func (cc *ChainClient) CreateSetInvokeGasPricePayload(gasPrice string) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] begin CreateSetInvokeGasPricePayload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeySetInvokeGasPrice,
			Value: []byte(gasPrice),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, err
	}

	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_SET_INVOKE_GAS_PRICE.String(), pairs, seq+1, nil), nil
}

// CreateSetInstallBaseGasPayload create set_install_base_gas payload
func (cc *ChainClient) CreateSetInstallBaseGasPayload(amount int64) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] begin CreateSetInvokeBaseGasPayload")

	if amount < 0 {
		return nil, errors.New("amount must >= 0")
	}

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeySetInstallBaseGas,
			Value: []byte(strconv.FormatInt(amount, 10)),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, err
	}

	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_SET_INSTALL_BASE_GAS.String(), pairs, seq+1, nil), nil
}

// CreateSetInstallGasPricePayload create set_install_gas_price payload
func (cc *ChainClient) CreateSetInstallGasPricePayload(gasPrice string) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] begin CreateSetInvokeBaseGasPayload")

	pairs := []*common.KeyValuePair{
		{
			Key:   utils.KeySetInstallGasPrice,
			Value: []byte(gasPrice),
		},
	}

	seq, err := cc.GetChainConfigSequence()
	if err != nil {
		return nil, err
	}

	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CHAIN_CONFIG.String(),
		syscontract.ChainConfigFunction_SET_INSTALL_GAS_PRICE.String(), pairs, seq+1, nil), nil
}

// estimateGasPayload create estimate gas payload for query
func (cc *ChainClient) estimateGasPayload(userPayload *common.Payload) (*common.Payload, error) {
	// 现有处理逻辑会导致原payload被修改，导致用户无法使用
	// 本身因为TxType的修改，需要重新签名，因此需要重新构建payload
	payloadBytes, err := proto.Marshal(userPayload)
	if err != nil {
		return nil, err
	}
	var estimateGasPayload common.Payload
	err = proto.Unmarshal(payloadBytes, &estimateGasPayload)
	if err != nil {
		return nil, err
	}
	// 重新设置estimateGasPayload的部分参数
	estimateGasPayload.TxType = common.TxType_QUERY_CONTRACT
	// 构建新的txid
	if cc.enableNormalKey {
		estimateGasPayload.TxId = utils.GetRandTxId()
	} else {
		estimateGasPayload.TxId = utils.GetTimestampTxId()
	}
	return &estimateGasPayload, nil
}
