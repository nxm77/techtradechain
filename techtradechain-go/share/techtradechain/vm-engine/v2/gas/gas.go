package gas

import (
	"errors"

	protocol "techtradechain.com/techtradechain/protocol/v2"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	gasutils "techtradechain.com/techtradechain/utils/v2/gas"
)

const (
	blockVersion2312 = uint32(2030102)
)

// GetSenderAddressGasUsed returns get sender address gas used
func GetSenderAddressGasUsed(gasUsed uint64) (uint64, error) {
	gasUsed += 10 * GetSenderAddressGasPrice
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

// PutStateGasUsed returns put state gas used
func PutStateGasUsed(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	gasUsed uint64, contractName, key, field string, value []byte) (uint64, error) {

	if blockVersion < blockVersion2312 {
		return PutStateGasUsedLt2312(gasUsed, contractName, key, field, value)
	}

	return PutStateGasUsed2312(gasConfig, gasUsed, contractName, key, field, value)
}

// GetStateGasUsed returns put state gas used
func GetStateGasUsed(
	blockVersion uint32, gasConfig *gasutils.GasConfig, gasUsed uint64,
	contractName, key, field string, value []byte) (uint64, error) {

	if blockVersion < blockVersion2312 {
		return GetStateGasUsedLt2312(gasUsed, value)
	}

	return GetStateGasUsed2312(gasConfig, gasUsed, contractName, key, field, value)
}

// GetBatchStateGasUsed returns get batch state gas used
func GetBatchStateGasUsed(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	gasUsed uint64, payload []byte) (uint64, error) {

	if blockVersion < blockVersion2312 {
		return GetBatchStateGasUsedLt2312(gasUsed, payload)
	}

	return GetBatchStateGasUsed2312(gasConfig, gasUsed, payload)
}

// EmitEventGasUsed returns emit event gas used
func EmitEventGasUsed(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	gasUsed uint64, contractEvent *common.ContractEvent) (uint64, error) {

	if blockVersion < blockVersion2312 {
		return EmitEventGasUsedLt2312(gasUsed, contractEvent)
	}

	return EmitEventGasUsed2312(gasConfig, gasUsed, contractEvent)
}

// CreateKeyHistoryIterGasUsed returns create key history iter gas used
func CreateKeyHistoryIterGasUsed(blockVersion uint32, gasConfig *gasutils.GasConfig,
	params map[string][]byte, gasUsed uint64, txId string, log protocol.Logger) (uint64, error) {
	if blockVersion < blockVersion2312 {
		return CreateKeyHistoryIterGasUsedLt2312(gasUsed)
	}
	return CreateKeyHistoryIterGasUsed2312(gasConfig, params, gasUsed, txId, log)
}

// ConsumeKeyHistoryIterGasUsed returns consume key history iter gas used
func ConsumeKeyHistoryIterGasUsed(blockVersion uint32, gasConfig *gasutils.GasConfig, gasUsed uint64) (uint64, error) {
	if blockVersion < blockVersion2312 {
		return ConsumeKeyHistoryIterGasUsedLt2312(gasUsed)
	}
	return ConsumeKeyHistoryIterGasUsed2312(gasConfig, gasUsed)
}

// CreateKvIteratorGasUsed create kv iter gas used
func CreateKvIteratorGasUsed(blockVersion uint32, gasConfig *gasutils.GasConfig,
	params map[string][]byte, gasUsed uint64, txId string, log protocol.Logger) (uint64, error) {
	if blockVersion < blockVersion2312 {
		return CreateKvIteratorGasUsedLt2312(gasUsed)
	}
	return CreateKvIteratorGasUsed2312(gasConfig, params, gasUsed, txId, log)
}

// ConsumeKvIteratorGasUsed returns kv iter gas used
func ConsumeKvIteratorGasUsed(blockVersion uint32, gasConfig *gasutils.GasConfig, gasUsed uint64) (uint64, error) {
	if blockVersion < blockVersion2312 {
		return ConsumeKvIteratorGasUsedLt2312(gasUsed)
	}

	return ConsumeKvIteratorGasUsed2312(gasConfig, gasUsed)
}

// CallContractGasUsed return call contract gas used
func CallContractGasUsed(blockVersion uint32, gasConfig *gasutils.GasConfig, gasUsed uint64,
	contractName string, contractMethod string, parameters map[string][]byte,
	txId string, log protocol.Logger) (uint64, error) {
	if blockVersion < blockVersion2312 {
		return gasUsed, nil
	}

	return CallContractGasUsed2312(gasConfig, gasUsed, contractName, contractMethod, parameters, txId, log)
}
