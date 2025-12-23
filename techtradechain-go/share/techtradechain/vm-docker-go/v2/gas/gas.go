/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gas

import (
	"encoding/json"
	"errors"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
)

const (
	// function list gas price
	GetArgsGasPrice               uint64 = 1
	GetStateGasPrice              uint64 = 1
	GetBatchStateGasPrice         uint64 = 1
	PutStateGasPrice              uint64 = 10
	DelStateGasPrice              uint64 = 10
	GetCreatorOrgIdGasPrice       uint64 = 1
	GetCreatorRoleGasPrice        uint64 = 1
	GetCreatorPkGasPrice          uint64 = 1
	GetSenderOrgIdGasPrice        uint64 = 1
	GetSenderRoleGasPrice         uint64 = 1
	GetSenderPkGasPrice           uint64 = 1
	GetBlockHeightGasPrice        uint64 = 1
	GetTxIdGasPrice               uint64 = 1
	GetTimeStampPrice             uint64 = 1
	EmitEventGasPrice             uint64 = 5
	LogGasPrice                   uint64 = 5
	KvIteratorCreateGasPrice      uint64 = 1
	KvPreIteratorCreateGasPrice   uint64 = 1
	KvIteratorHasNextGasPrice     uint64 = 1
	KvIteratorNextGasPrice        uint64 = 1
	KvIteratorCloseGasPrice       uint64 = 1
	KeyHistoryIterCreateGasPrice  uint64 = 1
	KeyHistoryIterHasNextGasPrice uint64 = 1
	KeyHistoryIterNextGasPrice    uint64 = 1
	KeyHistoryIterCloseGasPrice   uint64 = 1
	GetSenderAddressGasPrice      uint64 = 1

	// special parameters passed to contract
	ContractParamCreatorOrgId = "__creator_org_id__"
	ContractParamCreatorRole  = "__creator_role__"
	ContractParamCreatorPk    = "__creator_pk__"
	ContractParamSenderOrgId  = "__sender_org_id__"
	ContractParamSenderRole   = "__sender_role__"
	ContractParamSenderPk     = "__sender_pk__"
	ContractParamBlockHeight  = "__block_height__"
	ContractParamTxId         = "__tx_id__"
	ContractParamTxTimeStamp  = "__tx_time_stamp__"

	// method
	initContract    = "init_contract"
	upgradeContract = "upgrade"

	// upgrade contract base gas used
	calcBaseGas uint64 = 1000

	// invoke contract base gas used
	invokeBaseGas uint64 = 10000

	// parameters base gas used
	paramsBaseGas uint64 = 1250
)

func GetArgsGasUsed(gasUsed uint64, args map[string]string) (uint64, error) {
	argsBytes, err := json.Marshal(args)
	if err != nil {
		return 0, err
	}
	gasUsed += uint64(len(argsBytes)) * GetArgsGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

func GetSenderAddressGasUsed(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * GetSenderAddressGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

func CreateKeyHistoryIterGasUsed(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * KeyHistoryIterCreateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

func ConsumeKeyHistoryIterGasUsed(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * KeyHistoryIterHasNextGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

func CreateKvIteratorGasUsed(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * KvIteratorCreateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

func ConsumeKvIteratorGasUsed(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * KvIteratorNextGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}

	return gasUsed, nil
}

func GetStateGasUsed(gasUsed uint64, value []byte) (uint64, error) {
	gasUsed += uint64(len(value)) * GetStateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

func GetBatchStateGasUsed(gasUsed uint64, payload []byte) (uint64, error) {
	gasUsed += uint64(len(payload)) * GetBatchStateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

func PutStateGasUsed(gasUsed uint64, contractName, key, field string, value []byte) (uint64, error) {
	gasUsed += (uint64(len(value)) + uint64(len([]byte(contractName+key+field)))) * PutStateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

func DelStateGasUsed(gasUsed uint64, value []byte) (uint64, error) {
	gasUsed += uint64(len(value)) * DelStateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

func EmitEventGasUsed(gasUsed uint64, contractEvent *common.ContractEvent) (uint64, error) {
	contractEventBytes, err := json.Marshal(contractEvent)
	if err != nil {
		return 0, err
	}

	gasUsed += uint64(len(contractEventBytes)) * EmitEventGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

func InitFuncGasUsed(gasUsed uint64, parameters map[string][]byte) (uint64, error) {
	if !checkKeys(parameters) {
		return 0, errors.New("check init key exist")
	}

	gasUsed = gasUsed + invokeBaseGas + paramsBaseGas
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}

	return gasUsed, nil
}

func InitFuncGasUsedLT2310(gasUsed uint64, parameters map[string][]byte) (uint64, error) {
	if !checkKeys(parameters) {
		return 0, errors.New("check init key exist")
	}

	gasUsed = getInitFuncGasUsedLT2310(gasUsed, parameters)
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}

	return gasUsed, nil
}

func ContractGasUsed(txSimContext protocol.TxSimContext, gasUsed uint64, method string,
	contractName string, byteCode []byte) (uint64, error) {
	if method == initContract {
		gasUsed += (uint64(len([]byte(contractName+utils.PrefixContractByteCode))) +
			uint64(len(byteCode))) * PutStateGasPrice
	}

	blockVersion := txSimContext.GetBlockVersion()
	if method == upgradeContract {
		if blockVersion < 220 {
			oldByteCode, err := txSimContext.Get(contractName, []byte(utils.PrefixContractByteCode))
			if err != nil {
				return 0, err
			}
			gasUsed += upgradeContractGasUsed(gasUsed, byteCode, oldByteCode)
		} else {
			gasUsed += uint64(len(byteCode)) * PutStateGasPrice
		}
	}

	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

func upgradeContractGasUsed(gasUsed uint64, byteCode, oldByteCode []byte) uint64 {
	diff := len(byteCode) - len(oldByteCode)
	if diff < 0 {
		gasUsed += calcBaseGas
	} else {
		gasUsed += uint64(diff) * PutStateGasPrice
	}
	return gasUsed
}

func checkKeys(args map[string][]byte) bool {
	keys := []string{
		ContractParamCreatorOrgId,
		ContractParamCreatorRole,
		ContractParamCreatorPk,
		ContractParamSenderOrgId,
		ContractParamSenderRole,
		ContractParamSenderPk,
		ContractParamBlockHeight,
		ContractParamTxId,
		ContractParamTxTimeStamp,
	}
	for _, key := range keys {
		if _, ok := args[key]; !ok {
			return false
		}
	}
	return true
}

func getInitFuncGasUsedLT2310(gasUsed uint64, args map[string][]byte) uint64 {
	return gasUsed +
		invokeBaseGas +
		uint64(len(args[ContractParamCreatorOrgId]))*GetCreatorOrgIdGasPrice +
		uint64(len(args[ContractParamBlockHeight]))*GetBlockHeightGasPrice +
		uint64(len(args[ContractParamCreatorPk]))*GetCreatorPkGasPrice +
		uint64(len(args[ContractParamCreatorRole]))*GetCreatorRoleGasPrice +
		uint64(len(args[ContractParamSenderOrgId]))*GetSenderOrgIdGasPrice +
		uint64(len(args[ContractParamTxId]))*GetTxIdGasPrice +
		uint64(len(args[ContractParamSenderRole]))*GetSenderRoleGasPrice +
		uint64(len(args[ContractParamSenderPk]))*GetSenderPkGasPrice +
		uint64(len(args[ContractParamTxTimeStamp]))*GetTimeStampPrice
}

func CheckGasLimit(gasUsed uint64) bool {
	return gasUsed > protocol.GasLimit
}
