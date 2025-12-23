/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gas

import (
	"encoding/json"
	"errors"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	protocol "techtradechain.com/techtradechain/protocol/v2"
	utils "techtradechain.com/techtradechain/utils/v2"
)

// nolint: revive
const (
	// function list gas price
	GetArgsGasPrice               uint64 = 1  // GetArgsGasPrice
	GetStateGasPrice              uint64 = 1  // GetStateGasPrice
	GetBatchStateGasPrice         uint64 = 1  // GetBatchStateGasPrice
	PutStateGasPrice              uint64 = 10 // PutStateGasPrice
	DelStateGasPrice              uint64 = 10 // DelStateGasPrice
	GetCreatorOrgIdGasPrice       uint64 = 1  // GetCreatorOrgIdGasPrice
	GetCreatorRoleGasPrice        uint64 = 1  // GetCreatorRoleGasPrice
	GetCreatorPkGasPrice          uint64 = 1  // GetCreatorPkGasPrice
	GetSenderOrgIdGasPrice        uint64 = 1  //GetSenderOrgIdGasPrice
	GetSenderRoleGasPrice         uint64 = 1  //GetSenderRoleGasPrice
	GetSenderPkGasPrice           uint64 = 1  //GetSenderPkGasPrice
	GetBlockHeightGasPrice        uint64 = 1  //GetBlockHeightGasPrice
	GetTxIdGasPrice               uint64 = 1  //GetTxIdGasPrice
	GetTimeStampPrice             uint64 = 1  //GetTimeStampPrice
	EmitEventGasPrice             uint64 = 5  //EmitEventGasPrice
	LogGasPrice                   uint64 = 5  //LogGasPrice
	KvIteratorCreateGasPrice      uint64 = 1  //KvIteratorCreateGasPrice
	KvPreIteratorCreateGasPrice   uint64 = 1  //KvPreIteratorCreateGasPrice
	KvIteratorHasNextGasPrice     uint64 = 1  //KvIteratorHasNextGasPrice
	KvIteratorNextGasPrice        uint64 = 1  //KvIteratorNextGasPrice
	KvIteratorCloseGasPrice       uint64 = 1  //KvIteratorCloseGasPrice
	KeyHistoryIterCreateGasPrice  uint64 = 1  //KeyHistoryIterCreateGasPrice
	KeyHistoryIterHasNextGasPrice uint64 = 1  //KeyHistoryIterHasNextGasPrice
	KeyHistoryIterNextGasPrice    uint64 = 1  //KeyHistoryIterNextGasPrice
	KeyHistoryIterCloseGasPrice   uint64 = 1  //KeyHistoryIterCloseGasPrice
	GetSenderAddressGasPrice      uint64 = 1  //GetSenderAddressGasPrice

	// special parameters passed to contract
	ContractParamCreatorOrgId = "__creator_org_id__" //ContractParamCreatorOrgId
	ContractParamCreatorRole  = "__creator_role__"   //ContractParamCreatorRole
	ContractParamCreatorPk    = "__creator_pk__"     //ContractParamCreatorPk
	ContractParamSenderOrgId  = "__sender_org_id__"  //ContractParamSenderOrgId
	ContractParamSenderRole   = "__sender_role__"    //ContractParamSenderRole
	ContractParamSenderPk     = "__sender_pk__"      //ContractParamSenderPk
	ContractParamBlockHeight  = "__block_height__"   //ContractParamBlockHeight
	ContractParamTxId         = "__tx_id__"          //ContractParamTxId
	ContractParamTxTimeStamp  = "__tx_time_stamp__"  //ContractParamTxTimeStamp

	// method
	initContract    = "init_contract"
	upgradeContract = "upgrade"

	// invoke contract base gas used
	defaultInvokeBaseGas uint64 = 10000

	// init function gas used
	initFuncGas uint64 = 1250
)

// GetStateGasUsedLt2312 returns get state gas used
func GetStateGasUsedLt2312(gasUsed uint64, value []byte) (uint64, error) {
	gasUsed += uint64(len(value)) * GetStateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

// GetBatchStateGasUsedLt2312 returns get batch state gas used
func GetBatchStateGasUsedLt2312(gasUsed uint64, payload []byte) (uint64, error) {
	gasUsed += uint64(len(payload)) * GetBatchStateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

// PutStateGasUsedLt2312 returns put state gas used
func PutStateGasUsedLt2312(gasUsed uint64, contractName, key, field string, value []byte) (uint64, error) {
	gasUsed += (uint64(len(value)) + uint64(len([]byte(contractName+key+field)))) * PutStateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

// EmitEventGasUsedLt2312 returns emit event gas used
func EmitEventGasUsedLt2312(gasUsed uint64, contractEvent *common.ContractEvent) (uint64, error) {
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

// InitFuncGasUsedLt2312 returns init func gas used
func InitFuncGasUsedLt2312(gasUsed, configDefaultGas uint64) (uint64, error) {
	gasUsed = getInitFuncGasUsed(gasUsed, configDefaultGas)
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}

	return gasUsed, nil
}

// ContractGasUsed returns contract gas used
func ContractGasUsed(gasUsed uint64, method string, contractName string, byteCode []byte) (uint64, error) {
	if method == initContract {
		gasUsed += (uint64(len([]byte(contractName+utils.PrefixContractByteCode))) +
			uint64(len(byteCode))) * PutStateGasPrice
	}

	if method == upgradeContract {
		gasUsed += uint64(len(byteCode)) * PutStateGasPrice
	}

	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited ")
	}
	return gasUsed, nil
}

func getInitFuncGasUsed(gasUsed, configDefaultGas uint64) uint64 {
	// if config not set default gas
	if configDefaultGas == 0 {
		return gasUsed + defaultInvokeBaseGas + initFuncGas
	}
	return gasUsed + configDefaultGas + initFuncGas

}

// CheckGasLimit judge gas limit enough
func CheckGasLimit(gasUsed uint64) bool {
	return gasUsed > protocol.GasLimit
}

// CreateKeyHistoryIterGasUsedLt2312 returns create key history iter gas used
func CreateKeyHistoryIterGasUsedLt2312(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * KeyHistoryIterCreateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

// ConsumeKeyHistoryIterGasUsedLt2312 returns consume key history iter gas used
func ConsumeKeyHistoryIterGasUsedLt2312(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * KeyHistoryIterHasNextGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

// CreateKvIteratorGasUsedLt2312 create kv iter gas used
func CreateKvIteratorGasUsedLt2312(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * KvIteratorCreateGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

// ConsumeKvIteratorGasUsedLt2312 returns kv iter gas used
func ConsumeKvIteratorGasUsedLt2312(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * KvIteratorNextGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}

	return gasUsed, nil
}
