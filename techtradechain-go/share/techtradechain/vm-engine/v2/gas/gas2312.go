package gas

import (
	"errors"
	"strings"

	"techtradechain.com/techtradechain/vm-engine/v2/config"

	protocol "techtradechain.com/techtradechain/protocol/v2"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	gasutils "techtradechain.com/techtradechain/utils/v2/gas"
)

// PutStateGasUsed2312 returns put state gas used
func PutStateGasUsed2312(gasConfig *gasutils.GasConfig,
	gasUsed uint64, contractName, key, field string, value []byte) (uint64, error) {

	return gasUsed, nil
}

// GetStateGasUsed2312 returns get state gas used
func GetStateGasUsed2312(gasConfig *gasutils.GasConfig, gasUsed uint64,
	contractName, key, field string, value []byte) (uint64, error) {

	return gasUsed, nil
}

// GetBatchStateGasUsed2312 returns get batch state gas used
func GetBatchStateGasUsed2312(gasConfig *gasutils.GasConfig, gasUsed uint64, payload []byte) (uint64, error) {

	return gasUsed, nil
}

// EmitEventGasUsed2312 returns emit event gas used
func EmitEventGasUsed2312(gasConfig *gasutils.GasConfig,
	gasUsed uint64, contractEvent *common.ContractEvent) (uint64, error) {

	return gasUsed, nil
}

// CreateKeyHistoryIterGasUsed2312 calculate gas for key history iterator `Create` operation
func CreateKeyHistoryIterGasUsed2312(gasConfig *gasutils.GasConfig,
	params map[string][]byte, gasUsed uint64, txId string, log protocol.Logger) (uint64, error) {

	gasPrice := float32(0)
	if gasConfig != nil {
		gasPrice = gasConfig.GetGasPriceForInvoke()
	}
	dataSize := 0
	dataSize += len(params[config.KeyHistoryIterKey])
	dataSize += len(params[config.KeyHistoryIterField])

	log.Debugf("【gas calc】%v, CreateKvIteratorGasUsed2312, dataSize = %v", txId, dataSize)

	gas, err := gasutils.MultiplyGasPrice(dataSize, gasPrice)
	if err != nil {
		return 0, err
	}

	gasUsed += gas
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

// CreateKvIteratorGasUsed2312 calculate gas for key-value iterator `Create` operation
func CreateKvIteratorGasUsed2312(gasConfig *gasutils.GasConfig,
	params map[string][]byte, gasUsed uint64,
	txId string, log protocol.Logger) (uint64, error) {

	gasPrice := float32(0)
	if gasConfig != nil {
		gasPrice = gasConfig.GetGasPriceForInvoke()
	}
	dataSize := 0
	dataSize += len(params[config.KeyIterStartKey])
	dataSize += len(params[config.KeyIterStartField])
	dataSize += len(params[config.KeyIterLimitKey])
	dataSize += len(params[config.KeyIterLimitField])

	log.Debugf("【gas calc】%v, CreateKvIteratorGasUsed2312, dataSize = %v", txId, dataSize)

	gas, err := gasutils.MultiplyGasPrice(dataSize, gasPrice)
	if err != nil {
		return 0, err
	}

	gasUsed += gas
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}

// ConsumeKeyHistoryIterGasUsed2312 calculate gas for key history iterator `HasNext/Close` operation
func ConsumeKeyHistoryIterGasUsed2312(gasConfig *gasutils.GasConfig, gasUsed uint64) (uint64, error) {

	return gasUsed, nil
}

// ConsumeKvIteratorGasUsed2312 calculate gas for key-value iterator `HasNext/Close` operation
func ConsumeKvIteratorGasUsed2312(gasConfig *gasutils.GasConfig, gasUsed uint64) (uint64, error) {
	return gasUsed, nil
}

// ConsumeKvIteratorNextGasUsed2312 calculate gas for key-value iterator `Next` operation
func ConsumeKvIteratorNextGasUsed2312(gasConfig *gasutils.GasConfig, gasUsed uint64,
	key string, field string, value []byte) (uint64, error) {

	return gasUsed, nil
}

// ConsumeKeyHistoryIterNextGasUsed2312 calculate gas for key-value iterator `Next` operation
func ConsumeKeyHistoryIterNextGasUsed2312(gasConfig *gasutils.GasConfig, gasUsed uint64,
	value []byte) (uint64, error) {

	return gasUsed, nil
}

// CallContractGasUsed2312 calculate gas for calling contract
func CallContractGasUsed2312(gasConfig *gasutils.GasConfig, gasUsed uint64,
	contractName string, contractMethod string, parameters map[string][]byte,
	txId string, log protocol.Logger) (uint64, error) {
	gasPrice := float32(0)
	if gasConfig != nil {
		gasPrice = gasConfig.GetGasPriceForInvoke()
	}
	dataSize := len(contractName) + len(contractMethod)
	log.Debugf("【gas calc】%v, len(%v) + len(%v) = %v", txId, contractName, contractMethod, dataSize)
	for key, val := range parameters {
		if strings.HasPrefix(key, "__") && strings.HasSuffix(key, "__") {
			continue
		}
		dataSize += len(key) + len(val)
		log.Debugf("【gas calc】%v, len(%v) + len(%v) = %v", txId, key, string(val), dataSize)
	}

	gas, err := gasutils.MultiplyGasPrice(dataSize, gasPrice)
	if err != nil {
		return 0, err
	}

	gasUsed += gas
	if CheckGasLimit(gasUsed) {
		return 0, errors.New("over gas limited")
	}
	return gasUsed, nil
}
