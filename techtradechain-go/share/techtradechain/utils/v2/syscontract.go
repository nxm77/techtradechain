/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

import (
	"strings"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
)

const (
	// Separator split the contract and method
	Separator = ":"
	// PrefixContractInfo prefix of contract info
	PrefixContractInfo = "Contract:"
	// PrefixContractByteCode prefix of contract bytecode
	PrefixContractByteCode = "ContractByteCode:"
	// PrefixContractMethodPayer prefix of contract method's payer address
	PrefixContractMethodPayer = "Payer:"
)

// GetContractDbKey get contract db key
func GetContractDbKey(contractName string) []byte {
	return []byte(PrefixContractInfo + contractName)
}

// GetContractByteCodeDbKey get contract byte code db key
func GetContractByteCodeDbKey(contractName string) []byte {
	return []byte(PrefixContractByteCode + contractName)
}

// GetContractByName get contract by name
func GetContractByName(readObject func(contractName string, key []byte) ([]byte, error), name string) (
	*commonPb.Contract, error) {
	key := GetContractDbKey(name)
	value, err := readObject(syscontract.SystemContract_CONTRACT_MANAGE.String(), key)
	if err != nil {
		return nil, err
	}
	contract := &commonPb.Contract{}
	err = contract.Unmarshal(value)
	if err != nil {
		return nil, err
	}
	return contract, nil
}

// GetContractBytecode get contract bytecode
func GetContractBytecode(readObject func(contractName string, key []byte) ([]byte, error), name string) (
	[]byte, error) {
	key := GetContractByteCodeDbKey(name)
	return readObject(syscontract.SystemContract_CONTRACT_MANAGE.String(), key)
}

// IsNativeContract return is native contract name
func IsNativeContract(contractName string) bool {
	_, ok := syscontract.SystemContract_value[contractName]
	return ok
}

// GetContractMethodPayerDbKey return the key of contract + method => payer mapping
func GetContractMethodPayerDbKey(contractName string, method string) []byte {
	dbKey := PrefixContractMethodPayer + contractName
	method = strings.TrimSpace(method)
	if len(method) > 0 {
		dbKey += Separator + method
	}

	return []byte(dbKey)
}

// GetContractMethodPayerPK return a payer public key of invoking contract
func GetContractMethodPayerPK(snapshot protocol.Snapshot, contractName string, method string) ([]byte, []byte, error) {

	contractDbKey := PrefixContractMethodPayer + contractName
	method = strings.TrimSpace(method)
	// method 为空，返回合约默认的 Payer
	if len(method) == 0 {
		v, err := snapshot.GetKey(-1,
			syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(contractDbKey))
		return []byte(contractDbKey), v, err
	}

	// method 不为空，先按照 {contractName}:{method} 查询
	methodDbKey := contractDbKey + Separator + method
	payer, err := snapshot.GetKey(-1,
		syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(methodDbKey))
	if err != nil {
		return nil, nil, err
	}
	if payer != nil {
		return []byte(methodDbKey), payer, nil
	}

	// 再按照 {contractName} 查询
	payer, err = snapshot.GetKey(-1,
		syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(contractDbKey))
	if err != nil {
		return nil, nil, err
	}

	return []byte(contractDbKey), payer, nil
}

// GetContractMethodPayerPKFromAC return a payer public key of invoking contract
func GetContractMethodPayerPKFromAC(ac protocol.AccessControlProvider,
	contractName string, method string) (
	[]byte, []byte, error) {

	contractDbKey := PrefixContractMethodPayer + contractName
	method = strings.TrimSpace(method)
	// method 为空，返回合约默认的 Payer
	if len(method) == 0 {
		//return snapshot.GetKey(-1,
		//	syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(contractDbKey))

		v, err := ac.GetPayerFromCache([]byte(contractDbKey))
		return []byte(contractDbKey), v, err
	}

	// method 不为空，先按照 {contractName}:{method} 查询
	methodDbKey := contractDbKey + Separator + method
	//payer, err := snapshot.GetKey(-1,
	//	syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(methodDbKey))
	payer, _ := ac.GetPayerFromCache([]byte(methodDbKey))
	if payer != nil {
		return []byte(methodDbKey), payer, nil
	}

	// 再按照 {contractName} 查询
	//payer, err = snapshot.GetKey(-1,
	//	syscontract.SystemContract_ACCOUNT_MANAGER.String(), []byte(contractDbKey))
	payer, err := ac.GetPayerFromCache([]byte(contractDbKey))
	if err != nil {
		return nil, nil, err
	}

	return []byte(contractDbKey), payer, nil
}
