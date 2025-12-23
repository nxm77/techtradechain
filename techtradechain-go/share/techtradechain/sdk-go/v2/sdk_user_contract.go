/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"strings"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
)

// CreateContractCreatePayload create user contract creation payload
func (cc *ChainClient) CreateContractCreatePayload(contractName, version, byteCodeStringOrFilePath string,
	runtime common.RuntimeType, kvs []*common.KeyValuePair) (*common.Payload, error) {

	cc.logger.Debugf("[SDK] create [ContractCreate] to be signed payload")
	return cc.createContractManageWithByteCodePayload(contractName,
		syscontract.ContractManageFunction_INIT_CONTRACT.String(), version, byteCodeStringOrFilePath, runtime, kvs)
}

// CreateContractUpgradePayload create user contract upgrade payload
func (cc *ChainClient) CreateContractUpgradePayload(contractName, version, byteCodeStringOrFilePath string,
	runtime common.RuntimeType, kvs []*common.KeyValuePair) (*common.Payload, error) {

	cc.logger.Debugf("[SDK] create [ContractUpgrade] to be signed payload")
	return cc.createContractManageWithByteCodePayload(contractName,
		syscontract.ContractManageFunction_UPGRADE_CONTRACT.String(), version, byteCodeStringOrFilePath, runtime, kvs)
}

// CreateContractFreezePayload create user contract freeze payload
func (cc *ChainClient) CreateContractFreezePayload(contractName string) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [ContractFreeze] to be signed payload")
	return cc.createContractManagePayload(contractName, syscontract.ContractManageFunction_FREEZE_CONTRACT.String())
}

// CreateContractUnfreezePayload create user contract unfreeze payload
func (cc *ChainClient) CreateContractUnfreezePayload(contractName string) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [ContractUnfreeze] to be signed payload")
	return cc.createContractManagePayload(contractName, syscontract.ContractManageFunction_UNFREEZE_CONTRACT.String())
}

// CreateContractRevokePayload create user contract revoke payload
func (cc *ChainClient) CreateContractRevokePayload(contractName string) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [ContractRevoke] to be signed payload")
	return cc.createContractManagePayload(contractName, syscontract.ContractManageFunction_REVOKE_CONTRACT.String())
}

func (cc *ChainClient) createContractManagePayload(contractName, method string) (*common.Payload, error) {
	kvs := []*common.KeyValuePair{
		{
			Key:   syscontract.GetContractInfo_CONTRACT_NAME.String(),
			Value: []byte(contractName),
		},
	}
	return cc.CreatePayload("", common.TxType_INVOKE_CONTRACT, syscontract.SystemContract_CONTRACT_MANAGE.String(),
		method, kvs, defaultSeq, nil), nil
}

func (cc *ChainClient) createContractManageWithByteCodePayload(contractName, method, version,
	byteCodeStringOrFilePath string, runtime common.RuntimeType, kvs []*common.KeyValuePair) (*common.Payload, error) {
	var (
		err       error
		codeBytes []byte
	)

	isFile := utils.Exists(byteCodeStringOrFilePath)
	if isFile {
		bz, err := ioutil.ReadFile(byteCodeStringOrFilePath)
		if err != nil {
			return nil, fmt.Errorf("read from byteCode file %s failed, %s", byteCodeStringOrFilePath, err)
		}

		if runtime == common.RuntimeType_EVM { // evm contract hex need decode to bytes
			codeBytesStr := strings.TrimSpace(string(bz))
			if codeBytes, err = hex.DecodeString(codeBytesStr); err != nil {
				return nil, fmt.Errorf("decode evm contract hex to bytes failed, %s", err)
			}
		} else { // wasm bin file no need decode
			codeBytes = bz
		}
	} else {
		if runtime == common.RuntimeType_EVM {
			byteCodeStringOrFilePath = strings.TrimSpace(byteCodeStringOrFilePath)
		}

		if codeBytes, err = hex.DecodeString(byteCodeStringOrFilePath); err != nil {
			if codeBytes, err = base64.StdEncoding.DecodeString(byteCodeStringOrFilePath); err != nil {
				return nil, fmt.Errorf("decode byteCode string failed, %s", err)
			}
		}
	}

	if !cc.checkKeyValuePair(kvs) {
		return nil, fmt.Errorf("use reserved word")
	}

	payload := cc.CreatePayload("", common.TxType_INVOKE_CONTRACT,
		syscontract.SystemContract_CONTRACT_MANAGE.String(), method, kvs, defaultSeq, nil)

	payload.Parameters = append(payload.Parameters, &common.KeyValuePair{
		Key:   syscontract.InitContract_CONTRACT_NAME.String(),
		Value: []byte(contractName),
	})

	payload.Parameters = append(payload.Parameters, &common.KeyValuePair{
		Key:   syscontract.InitContract_CONTRACT_VERSION.String(),
		Value: []byte(version),
	})

	payload.Parameters = append(payload.Parameters, &common.KeyValuePair{
		Key:   syscontract.InitContract_CONTRACT_RUNTIME_TYPE.String(),
		Value: []byte(runtime.String()),
	})

	payload.Parameters = append(payload.Parameters, &common.KeyValuePair{
		Key:   syscontract.InitContract_CONTRACT_BYTECODE.String(),
		Value: codeBytes,
	})

	return payload, nil
}

func (cc *ChainClient) checkKeyValuePair(kvs []*common.KeyValuePair) bool {
	for _, kv := range kvs {
		if kv.Key == syscontract.InitContract_CONTRACT_NAME.String() ||
			kv.Key == syscontract.InitContract_CONTRACT_RUNTIME_TYPE.String() ||
			kv.Key == syscontract.InitContract_CONTRACT_VERSION.String() ||
			kv.Key == syscontract.InitContract_CONTRACT_BYTECODE.String() ||
			kv.Key == syscontract.UpgradeContract_CONTRACT_NAME.String() ||
			kv.Key == syscontract.UpgradeContract_CONTRACT_RUNTIME_TYPE.String() ||
			kv.Key == syscontract.UpgradeContract_CONTRACT_VERSION.String() ||
			kv.Key == syscontract.UpgradeContract_CONTRACT_BYTECODE.String() {
			return false
		}
	}

	return true
}

// SignContractManagePayload sign user contract manage payload
func (cc *ChainClient) SignContractManagePayload(payload *common.Payload) (*common.EndorsementEntry, error) {
	return cc.SignPayload(payload)
}

// SendContractManageRequest send user contract manage request to node
func (cc *ChainClient) SendContractManageRequest(payload *common.Payload, endorsers []*common.EndorsementEntry,
	timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	return cc.proposalRequest(payload, endorsers, nil, timeout, withSyncResult)
}

// SendContractManageRequestWithPayer send user contract manage request to node with payer
func (cc *ChainClient) SendContractManageRequestWithPayer(payload *common.Payload, endorsers []*common.EndorsementEntry,
	payer *common.EndorsementEntry, timeout int64, withSyncResult bool) (*common.TxResponse, error) {
	return cc.proposalRequest(payload, endorsers, payer, timeout, withSyncResult)
}

// InvokeContract invoke contract
func (cc *ChainClient) InvokeContract(contractName, method, txId string, kvs []*common.KeyValuePair, timeout int64,
	withSyncResult bool) (*common.TxResponse, error) {
	return cc.InvokeContractWithLimit(contractName, method, txId, kvs, timeout, withSyncResult, nil)
}

// InvokeContractWithLimit invoke contract with specified gas limit
func (cc *ChainClient) InvokeContractWithLimit(contractName, method, txId string, kvs []*common.KeyValuePair,
	timeout int64, withSyncResult bool, limit *common.Limit) (*common.TxResponse, error) {

	cc.logger.Debugf("[SDK] begin to INVOKE contract, [contractName:%s]/[method:%s]/[txId:%s]/[params:%+v]",
		contractName, method, txId, kvs)

	payload := cc.CreatePayload(txId, common.TxType_INVOKE_CONTRACT, contractName, method, kvs, defaultSeq, limit)

	return cc.proposalRequest(payload, nil, nil, timeout, withSyncResult)
}

// InvokeContractBySigner invoke contract with specified signer.
// use signer to sign payload if it is not nil.
// use cc.privateKey to sign payload if signer is nil.
func (cc *ChainClient) InvokeContractBySigner(contractName, method, txId string, kvs []*common.KeyValuePair,
	timeout int64, withSyncResult bool, limit *common.Limit,
	signer Signer) (*common.TxResponse, error) {

	cc.logger.Debugf("[SDK] begin InvokeContractBySigner, [contractName:%s]/[method:%s]/[txId:%s]/[params:%+v]",
		contractName, method, txId, kvs)

	// construct payload
	payload := cc.CreatePayload(txId, common.TxType_INVOKE_CONTRACT, contractName, method, kvs, defaultSeq, limit)

	// construct tx req & sign
	req, err := cc.GenerateTxRequestBySigner(payload, nil, signer)
	if err != nil {
		return nil, err
	}

	// send tx req
	return cc.sendRequest(req, timeout, withSyncResult)
}

// QueryContract query contract
func (cc *ChainClient) QueryContract(contractName, method string, kvs []*common.KeyValuePair,
	timeout int64) (*common.TxResponse, error) {

	cc.logger.Debugf("[SDK] begin to QUERY contract, [contractName:%s]/[method:%s]/[params:%+v]",
		contractName, method, kvs)

	payload := cc.CreatePayload("", common.TxType_QUERY_CONTRACT, contractName, method, kvs, defaultSeq, nil)

	resp, err := cc.proposalRequest(payload, nil, nil, timeout, false)
	if err != nil {
		return nil, fmt.Errorf("send %s failed, %s", payload.TxType.String(), err.Error())
	}

	return resp, nil
}

// GetTxRequest build tx request, returns *common.TxRequest
func (cc *ChainClient) GetTxRequest(contractName, method, txId string, kvs []*common.KeyValuePair) (
	*common.TxRequest, error) {
	if txId == "" {
		if cc.enableNormalKey {
			txId = utils.GetRandTxId()
		} else {
			txId = utils.GetTimestampTxId()
		}
	}

	cc.logger.Debugf("[SDK] begin to create TxRequest, [contractName:%s]/[method:%s]/[txId:%s]/[params:%+v]",
		contractName, method, txId, kvs)

	payload := cc.CreatePayload(txId, common.TxType_INVOKE_CONTRACT, contractName, method, kvs, defaultSeq, nil)

	req, err := cc.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}

	return req, nil
}

// SendTxRequest send tx request to node
func (cc *ChainClient) SendTxRequest(txRequest *common.TxRequest, timeout int64,
	withSyncResult bool) (*common.TxResponse, error) {
	return cc.sendRequest(txRequest, timeout, withSyncResult)
}
