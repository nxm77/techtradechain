package docker_go

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil" //nolint:staticcheck
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"techtradechain.com/techtradechain/common/v2/bytehelper"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/store"
	vmPb "techtradechain.com/techtradechain/pb-go/v2/vm"
	protocol "techtradechain.com/techtradechain/protocol/v2"
	gasutils "techtradechain.com/techtradechain/utils/v2/gas"
	"techtradechain.com/techtradechain/vm-engine/v2/config"
	"techtradechain.com/techtradechain/vm-engine/v2/gas"
	"techtradechain.com/techtradechain/vm-engine/v2/pb/protogo"
	"techtradechain.com/techtradechain/vm-engine/v2/utils"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
)

const (
	blockVersion2312 = uint32(2030102)
)

func (r *RuntimeInstance) handleTxResponse(txId string, recvMsg *protogo.DockerVMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64, txType protocol.ExecOrderTxType, contractName string) (
	contractResult *commonPb.ContractResult, execOrderTxType protocol.ExecOrderTxType) {

	var err error
	txResponse := recvMsg.Response

	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	utils.EnterNextStep(recvMsg, protogo.StepType_RUNTIME_HANDLER_RECEIVE_TX_RESPONSE, func() string {
		return strings.Join([]string{"msgSize", strconv.Itoa(recvMsg.Size())}, ":")
	})

	defer func() {
		utils.EnterNextStep(recvMsg, protogo.StepType_RUNTIME_HANDLE_TX_RESPONSE, func() string {
			return ""
		})
		if str, ok := utils.PrintTxStepsWithTime(recvMsg); ok {
			r.logger.Warnf("[%s] slow tx execution, %s", recvMsg.TxId, str)
		}
	}()

	contractResult = new(commonPb.ContractResult)
	// tx fail, just return without merge read write map and events
	if txResponse.Code != 0 {
		contractResult.Code = 1
		contractResult.Result = txResponse.Result
		contractResult.Message = txResponse.Message
		contractResult.GasUsed = gasUsed

		return contractResult, protocol.ExecOrderTxTypeNormal
	}

	contractResult.Code = 0
	contractResult.Result = txResponse.Result
	contractResult.Message = txResponse.Message

	//txSimContext中，从写集读取的数据不会再放入读集，避免失败交易已经修改的数据写入读集，从而造成合约执行问题
	//vm-engine内为交易执行创建sandbox，sandbox为交易的读写集创建了缓存，这样执行合约时可以直接在本地缓存获取，不用通过rpc请求从链上获取，
	//从sandbox的写集内读取了kv也是会写入sandbox读集的，那么将sandbox的读集合并到txSimContext的读集时，就又造成了写集的数据写入了读集，
	//将读集合并取消后，读集完全由txSimContext维护
	if txSimContext.GetBlockVersion() < version235 {
		// merge read map to sim context
		if err = r.mergeSimContextReadMap(txSimContext, txResponse.GetReadMap(), contractName); err != nil {
			r.logger.Errorf("fail to merge tx[%s] sim context read map, %v", txId, err)
			return r.errorResult(contractResult, err, "fail to put in sim context")
		}
		r.logger.DebugDynamic(func() string {
			return fmt.Sprintf("merge tx[%s] sim context read map succeed", txId)
		})
	}

	// merge write map to sim context
	gasUsed, err = r.mergeSimContextWriteMap(txSimContext, txResponse.GetWriteMap(), gasUsed, contractName)
	if err != nil {
		r.logger.Errorf("fail to merge tx[%s] sim context write map, %v", txId, err)
		contractResult.GasUsed = gasUsed
		return r.errorResult(contractResult, err, "fail to put in sim context")
	}
	r.logger.DebugDynamic(func() string {
		return fmt.Sprintf("merge tx[%s] sim context write map succeed", txId)
	})

	for _, event := range txResponse.Events {
		contractEvent := &commonPb.ContractEvent{
			Topic:        event.Topic,
			TxId:         txId,
			ContractName: event.ContractName,
			EventData:    event.Data,
		}

		// emit event gas used calc and check gas limit
		r.logger.Debugf("【gas calc】%v, before EmitEvent => gasUsed = %v, events = %v",
			txSimContext.GetTx().Payload.TxId, gasUsed, contractEvent)
		gasUsed, err = gas.EmitEventGasUsed(blockVersion, gasConfig, gasUsed, contractEvent)
		r.logger.Debugf("【gas calc】%v, after EmitEvent => gasUsed = %v",
			txSimContext.GetTx().Payload.TxId, gasUsed)
		if err != nil {
			contractResult.GasUsed = gasUsed
			return r.errorResult(contractResult, err, err.Error())
		}

		r.event = append(r.event, contractEvent)
	}

	contractResult.GasUsed = gasUsed
	contractResult.ContractEvent = r.event

	return contractResult, txType
}

func (r *RuntimeInstance) mergeSimContextReadMap(txSimContext protocol.TxSimContext,
	readMap map[string][]byte, txContractName string) error {

	for key, value := range readMap {
		var contractName string
		var contractKey string
		var contractField string
		keyList := strings.Split(key, "#")
		keyLen := len(keyList)
		if keyLen < 2 {
			return fmt.Errorf("%s's key list length == %d, needs to be >= 2", key, keyLen)
		}
		contractName = keyList[0]
		if contractName != txContractName {
			return fmt.Errorf("wrong contract name [%s] of read map key, need [%s]", contractName, txContractName)
		}
		contractKey = keyList[1]
		if keyLen == 3 {
			contractField = keyList[2]
		}

		txSimContext.PutIntoReadSet(contractName, protocol.GetKeyStr(contractKey, contractField), value)
	}
	return nil
}

func (r *RuntimeInstance) setError(errMsg string, response *protogo.DockerVMMessage) {
	r.logger.Error(errMsg)
	response.SysCallMessage.Code = protogo.DockerVMCode_FAIL
	response.SysCallMessage.Message = errMsg
}

func (r *RuntimeInstance) handlerCallContract(
	txId string,
	recvMsg *protogo.DockerVMMessage,
	txSimContext protocol.TxSimContext,
	gasUsed uint64,
	caller *commonPb.Contract,
) (*protogo.DockerVMMessage, uint64, protocol.ExecOrderTxType) {

	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	response := r.newEmptyResponse(txId, protogo.DockerVMType_CALL_CONTRACT_RESPONSE)
	specialTxType := protocol.ExecOrderTxTypeNormal
	// validate cross contract params
	callContractPayload := recvMsg.SysCallMessage.Payload[config.KeyCallContractReq]
	var callContractReq protogo.CallContractRequest
	err := proto.Unmarshal(callContractPayload, &callContractReq)
	if err != nil {
		response.SysCallMessage.Code = protogo.DockerVMCode_FAIL
		response.SysCallMessage.Message = err.Error()
		return response, gasUsed, specialTxType
	}

	contractName := callContractReq.ContractName
	if len(contractName) == 0 {
		errMsg := "missing contract name"
		r.setError(errMsg, response)
		return response, gasUsed, specialTxType
	}

	contractMethod := callContractReq.ContractMethod
	if len(contractMethod) == 0 {
		errMsg := "missing contract method"
		r.setError(errMsg, response)
		return response, gasUsed, specialTxType
	}

	if recvMsg.CrossContext.CurrentDepth > protocol.CallContractDepth {
		errMsg := "exceed max depth"
		r.setError(errMsg, response)
		return response, gasUsed, specialTxType
	}

	// construct new tx
	var result *commonPb.ContractResult
	var code commonPb.TxStatusCode
	var contract *commonPb.Contract
	contract, err = txSimContext.GetContractByName(contractName)
	if err != nil {
		errMsg := fmt.Sprintf(
			"[call contract] failed to get contract by [%s], err: %s",
			contractName,
			err.Error(),
		)
		r.setError(errMsg, response)
		return response, gasUsed, specialTxType
	}

	parameters := callContractReq.Args
	gasUsed, err = gas.CallContractGasUsed(blockVersion, gasConfig, gasUsed,
		contractName, contractMethod, parameters, txId, r.logger)
	if err != nil {
		errMsg := fmt.Sprintf(
			"[call contract] failed to subtract gas by [%s], err: %s",
			contractName,
			err.Error(),
		)
		r.setError(errMsg, response)
		return response, gasUsed, specialTxType
	}

	result, specialTxType, code = txSimContext.CallContract(caller, contract, contractMethod,
		nil, parameters, gasUsed, txSimContext.GetTx().Payload.TxType)
	r.logger.DebugDynamic(func() string {
		return fmt.Sprintf("call contract result [%+v]", result)
	})

	if code != commonPb.TxStatusCode_SUCCESS {
		errMsg := fmt.Sprintf("[call contract] execute error code: %s, msg: %s", code, result.Message)
		r.logger.Debugf("handle cross contract request failed, err: %s", errMsg)
		r.setError(errMsg, response)
		response.SysCallMessage.Payload = map[string][]byte{
			config.KeyCallContractResp: result.Result,
		}
		return response, gasUsed, specialTxType
	}

	response.SysCallMessage.Code = protogo.DockerVMCode_OK

	// merge event
	gasUsed = result.GasUsed
	for _, event := range result.ContractEvent {

		gasUsed, err = gas.EmitEventGasUsed(blockVersion, gasConfig, gasUsed, event)
		if err != nil {
			r.logger.Debugf("handle cross contract request failed, err: %s", err.Error())
			response.SysCallMessage.Code = protogo.DockerVMCode_FAIL
			response.SysCallMessage.Message = err.Error()
			return response, gasUsed, specialTxType
		}

		r.event = append(r.event, event)
	}

	var callContractResponse *protogo.ContractResponse
	callContractResponse, err = constructCallContractResponse(result, caller.Name, txSimContext)
	if err != nil {
		r.logger.Debugf("handle cross contract request failed, err: %s", err.Error())
		response.SysCallMessage.Code = protogo.DockerVMCode_FAIL
		response.SysCallMessage.Message = err.Error()
		return response, gasUsed, specialTxType
	}

	var respBytes []byte //nolint:typecheck
	respBytes, err = callContractResponse.Marshal()
	if err != nil {
		r.logger.Debugf("handle cross contract request failed, err: %s", err.Error())
		response.SysCallMessage.Code = protogo.DockerVMCode_FAIL
		response.SysCallMessage.Message = err.Error()
		return response, gasUsed, specialTxType
	}

	response.SysCallMessage.Payload = map[string][]byte{
		config.KeyCallContractResp: respBytes,
	}

	response.SysCallMessage.Code = protogo.DockerVMCode_OK
	//response.SysCallMessage.Message = "success"

	return response, gasUsed, specialTxType
}

func constructCallContractResponse(
	result *commonPb.ContractResult,
	contractName string,
	txSimContext protocol.TxSimContext,
) (*protogo.ContractResponse, error) {
	// get the latest status of the read / write set
	txReadMap, txWriteMap := txSimContext.GetTxRWMapByContractName(contractName)

	contractResponse := &protogo.ContractResponse{
		ReadMap:  make(map[string][]byte, len(txReadMap)),
		WriteMap: make(map[string][]byte, len(txWriteMap)),
		Response: &protogo.Response{
			Status:  int32(result.Code), //nolint:gosec
			Message: result.Message,
			Payload: result.Result,
		},
	}

	for readKey, txRead := range txReadMap {
		contractResponse.ReadMap[readKey] = txRead.Value
	}

	for writeKey, txWrite := range txWriteMap {
		contractResponse.WriteMap[writeKey] = txWrite.Value
	}

	return contractResponse, nil
}

func (r *RuntimeInstance) handleGetStateRequest(txId string, recvMsg *protogo.DockerVMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.DockerVMMessage, uint64) {

	response := r.newEmptyResponse(txId, protogo.DockerVMType_GET_STATE_RESPONSE)

	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	var contractName string
	var value []byte
	var err error

	contractNameBytes, ok := recvMsg.SysCallMessage.Payload[config.KeyContractName]
	if !ok {
		err = errors.New("unknown contract name")
		r.logger.Errorf("%s", err)
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		return response, gasUsed
	}
	stateKey := recvMsg.SysCallMessage.Payload[config.KeyStateKey]

	contractName = string(contractNameBytes)

	startTime := time.Now()
	value, err = txSimContext.Get(contractName, stateKey)
	if err != nil {
		r.logger.Errorf("fail to get state from sim context: %s", err)
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail

		r.currSysCall.AddStorageDuration(time.Since(startTime).Nanoseconds())

		return response, gasUsed
	}

	r.currSysCall.AddStorageDuration(time.Since(startTime).Nanoseconds())

	//r.logger.Debug("get value: ", string(value))
	r.logger.DebugDynamic(func() string {
		return fmt.Sprintf("[%s] get value: %s", txId, string(value))
	})

	response.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess

	// nil value in map[string][]byte will be empty []byte after grpc transfer, old version ignored this problem
	if txSimContext.GetBlockVersion() < version2310 || value != nil {
		response.SysCallMessage.Payload = map[string][]byte{
			config.KeyStateValue: value,
		}
	}

	keyList := strings.Split(string(stateKey), "#")
	var key, field string
	key = keyList[0]
	if len(keyList) == 2 {
		field = keyList[1]
	}
	gasUsed, err = gas.GetStateGasUsed(blockVersion, gasConfig, gasUsed, contractName, key, field, value)
	if err != nil {
		r.logger.Errorf("%s", err)
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		return response, gasUsed
	}

	return response, gasUsed
}

func (r *RuntimeInstance) handleGetBatchStateRequest(txId string, recvMsg *protogo.DockerVMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.DockerVMMessage, uint64) {

	response := r.newEmptyResponse(txId, protogo.DockerVMType_GET_BATCH_STATE_RESPONSE)
	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	var err error
	var payload []byte
	var getKeys []*vmPb.BatchKey

	keys := &vmPb.BatchKeys{}
	if err = keys.Unmarshal(recvMsg.SysCallMessage.Payload[config.KeyStateKey]); err != nil {
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		return response, gasUsed
	}

	startTime := time.Now()
	getKeys, err = txSimContext.GetKeys(keys.Keys)
	if err != nil {
		response.SysCallMessage.Message = err.Error()

		r.currSysCall.AddStorageDuration(time.Since(startTime).Nanoseconds())

		return response, gasUsed
	}

	r.currSysCall.AddStorageDuration(time.Since(startTime).Nanoseconds())

	r.logger.DebugDynamic(func() string {
		return fmt.Sprintf("get batch keys values: %v", getKeys)
	})
	resp := vmPb.BatchKeys{Keys: getKeys}
	payload, err = resp.Marshal()
	if err != nil {
		response.SysCallMessage.Message = err.Error()
		return response, gasUsed
	}

	response.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	response.SysCallMessage.Payload = map[string][]byte{
		config.KeyStateValue: payload,
	}
	gasUsed, err = gas.GetBatchStateGasUsed(blockVersion, gasConfig, gasUsed, payload)
	if err != nil {
		r.logger.Errorf("%s", err)
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		return response, gasUsed
	}

	return response, gasUsed
}

func (r *RuntimeInstance) handleCreateKvIterator(txId string, recvMsg *protogo.DockerVMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64, contractName string) (*protogo.DockerVMMessage, uint64) {

	createKvIteratorResponse := r.newEmptyResponse(txId, protogo.DockerVMType_CREATE_KV_ITERATOR_RESPONSE)
	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	r.logger.Debugf("【gas calc】%v, start create KvIterator => gasUsed = %v", txId, gasUsed)
	defer func() {
		r.logger.Debugf("【gas calc】%v, finish create KvIterator => gasUsed = %v", txId, gasUsed)
	}()

	/*
		|	index	|			desc			|
		|	----	|			----			|
		|	 0  	|		contractName		|
		|	 1  	|	createKvIteratorFunc	|
		|	 2  	|		startKey			|
		|	 3  	|		startField			|
		|	 4  	|		limitKey			|
		|	 5  	|		limitField			|
		|	 6  	|	  writeMapCache			|
	*/
	calledContractName := recvMsg.SysCallMessage.Payload[config.KeyContractName]
	createFunc := recvMsg.SysCallMessage.Payload[config.KeyIteratorFuncName]
	startKey := recvMsg.SysCallMessage.Payload[config.KeyIterStartKey]
	startField := recvMsg.SysCallMessage.Payload[config.KeyIterStartField]
	writeMapBytes := recvMsg.SysCallMessage.Payload[config.KeyWriteMap]

	writeMap := make(map[string][]byte)
	var err error
	if err = json.Unmarshal(writeMapBytes, &writeMap); err != nil {
		r.logger.Errorf("get WriteMap failed, %s", err.Error())
		createKvIteratorResponse.SysCallMessage.Message = err.Error()
		gasUsed, err = gas.CreateKvIteratorGasUsed(blockVersion, gasConfig,
			recvMsg.SysCallMessage.Payload, gasUsed, txId, r.logger)
		if err != nil {
			createKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.SysCallMessage.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	}

	gasUsed, err = r.mergeSimContextWriteMap(txSimContext, writeMap, gasUsed, contractName)
	if err != nil {
		r.logger.Errorf("merge the sim context write map failed, %s", err.Error())
		createKvIteratorResponse.SysCallMessage.Message = err.Error()
		gasUsed, err = gas.CreateKvIteratorGasUsed(blockVersion, gasConfig,
			recvMsg.SysCallMessage.Payload, gasUsed, txId, r.logger)
		if err != nil {
			createKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.SysCallMessage.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	}

	if err = protocol.CheckKeyFieldStr(string(startKey), string(startField)); err != nil {
		r.logger.Errorf("invalid key field str, %s", err.Error())
		createKvIteratorResponse.SysCallMessage.Message = err.Error()
		gasUsed, err = gas.CreateKvIteratorGasUsed(blockVersion, gasConfig,
			recvMsg.SysCallMessage.Payload, gasUsed, txId, r.logger)
		if err != nil {
			createKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.SysCallMessage.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	}

	key := protocol.GetKeyStr(string(startKey), string(startField))

	var iter protocol.StateIterator
	switch string(createFunc) {
	case config.FuncKvIteratorCreate:
		iter, gasUsed, err = kvIteratorCreate(txSimContext, gasConfig, string(calledContractName),
			recvMsg.SysCallMessage.Payload, gasUsed, txId, r.logger)
		if err != nil {
			r.logger.Errorf("failed to create kv iterator, %s", err.Error())
			createKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.SysCallMessage.Message = err.Error()
			createKvIteratorResponse.SysCallMessage.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	case config.FuncKvPreIteratorCreate:
		gasUsed, err = gas.CreateKvIteratorGasUsed(blockVersion, gasConfig,
			recvMsg.SysCallMessage.Payload, gasUsed, txId, r.logger)
		if err != nil {
			createKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.SysCallMessage.Message = err.Error()
			createKvIteratorResponse.SysCallMessage.Payload = nil
			return createKvIteratorResponse, gasUsed
		}

		keyStr := string(key)
		limitLast := keyStr[len(keyStr)-1] + 1
		limit := keyStr[:len(keyStr)-1] + string(limitLast)
		iter, err = txSimContext.Select(string(calledContractName), key, []byte(limit))
		if err != nil {
			r.logger.Errorf("failed to create kv pre iterator, %s", err.Error())
			createKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.SysCallMessage.Message = err.Error()
			createKvIteratorResponse.SysCallMessage.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	}

	index := atomic.AddInt32(&r.rowIndex, 1)
	txSimContext.SetIterHandle(index, iter)

	r.logger.Debug("create kv iterator: ", index)
	createKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	createKvIteratorResponse.SysCallMessage.Payload = map[string][]byte{
		config.KeyIterIndex: bytehelper.IntToBytes(index),
	}

	return createKvIteratorResponse, gasUsed
}

func (r *RuntimeInstance) handleConsumeKvIterator(txId string, recvMsg *protogo.DockerVMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.DockerVMMessage, uint64) {

	r.logger.Debugf("【gas calc】%v, start consume KvIterator => gasUsed = %v", txId, gasUsed)
	defer func() {
		r.logger.Debugf("【gas calc】%v, finish consume KvIterator => gasUsed = %v", txId, gasUsed)
	}()

	consumeKvIteratorResponse := r.newEmptyResponse(txId, protogo.DockerVMType_CONSUME_KV_ITERATOR_RESPONSE)
	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	/*
		|	index	|			desc				|
		|	----	|			----  				|
		|	 0  	|	consumeKvIteratorFunc		|
		|	 1  	|		rsIndex					|
	*/
	consumeKvIteratorFunc := recvMsg.SysCallMessage.Payload[config.KeyIteratorFuncName]
	kvIteratorIndex, err := bytehelper.BytesToInt(recvMsg.SysCallMessage.Payload[config.KeyIterIndex])
	if err != nil {
		r.logger.Errorf("failed to get iterator index, %s", err.Error())
		gasUsed, err = gas.ConsumeKvIteratorGasUsed(blockVersion, gasConfig, gasUsed)
		if err != nil {
			consumeKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			consumeKvIteratorResponse.SysCallMessage.Message = err.Error()
			consumeKvIteratorResponse.SysCallMessage.Payload = nil
			return consumeKvIteratorResponse, gasUsed
		}
		return consumeKvIteratorResponse, gasUsed
	}

	iter, ok := txSimContext.GetIterHandle(kvIteratorIndex)
	if !ok {
		r.logger.Errorf("[kv iterator consume] can not found iterator index [%d]", kvIteratorIndex)
		consumeKvIteratorResponse.SysCallMessage.Message = fmt.Sprintf(
			"[kv iterator consume] can not found iterator index [%d]", kvIteratorIndex,
		)

		gasUsed, err = gas.ConsumeKvIteratorGasUsed(blockVersion, gasConfig, gasUsed)
		if err != nil {
			consumeKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			consumeKvIteratorResponse.SysCallMessage.Message = err.Error()
			consumeKvIteratorResponse.SysCallMessage.Payload = nil
			return consumeKvIteratorResponse, gasUsed
		}
		return consumeKvIteratorResponse, gasUsed
	}

	kvIterator, ok := iter.(protocol.StateIterator)
	if !ok {
		r.logger.Errorf("assertion failed")
		consumeKvIteratorResponse.SysCallMessage.Message = fmt.Sprintf(
			"[kv iterator consume] failed, iterator %d assertion failed", kvIteratorIndex,
		)
		gasUsed, err = gas.ConsumeKvIteratorGasUsed(blockVersion, gasConfig, gasUsed)
		if err != nil {
			consumeKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			consumeKvIteratorResponse.SysCallMessage.Message = err.Error()
			consumeKvIteratorResponse.SysCallMessage.Payload = nil
			return consumeKvIteratorResponse, gasUsed
		}
		return consumeKvIteratorResponse, gasUsed
	}

	switch string(consumeKvIteratorFunc) {
	case config.FuncKvIteratorHasNext:
		return kvIteratorHasNext(blockVersion, gasConfig, kvIterator, gasUsed, consumeKvIteratorResponse)

	case config.FuncKvIteratorNext:
		return kvIteratorNext(blockVersion, gasConfig, kvIterator, gasUsed, consumeKvIteratorResponse)

	case config.FuncKvIteratorClose:
		return kvIteratorClose(blockVersion, gasConfig, kvIterator, gasUsed, consumeKvIteratorResponse)

	default:
		consumeKvIteratorResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		consumeKvIteratorResponse.SysCallMessage.Message = fmt.Sprintf("%s not found", consumeKvIteratorFunc)
		consumeKvIteratorResponse.SysCallMessage.Payload = nil
		return consumeKvIteratorResponse, gasUsed
	}
}

func (r *RuntimeInstance) handleCreateKeyHistoryIterator(txId string, recvMsg *protogo.DockerVMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64, contractName string) (*protogo.DockerVMMessage, uint64) {

	r.logger.Debugf("【gas calc】%v, start create KeyHistoryIterator => gasUsed = %v", txId, gasUsed)
	defer func() {
		r.logger.Debugf("【gas calc】%v, finish create KeyHistoryIterator => gasUsed = %v", txId, gasUsed)
	}()

	createKeyHistoryIterResponse := r.newEmptyResponse(txId, protogo.DockerVMType_CREATE_KEY_HISTORY_TER_RESPONSE)
	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	/*
		| index | desc          |
		| ----  | ----          |
		| 0     | contractName  |
		| 1     | key           |
		| 2     | field         |
		| 3     | writeMapCache |
	*/
	calledContractName := recvMsg.SysCallMessage.Payload[config.KeyContractName]
	keyStr := recvMsg.SysCallMessage.Payload[config.KeyHistoryIterKey]
	field := recvMsg.SysCallMessage.Payload[config.KeyHistoryIterField]
	writeMapBytes := recvMsg.SysCallMessage.Payload[config.KeyWriteMap]

	writeMap := make(map[string][]byte)
	var err error
	gasUsed, err = gas.CreateKeyHistoryIterGasUsed(blockVersion, gasConfig,
		recvMsg.SysCallMessage.Payload, gasUsed, txId, r.logger)
	if err != nil {
		createKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.SysCallMessage.Message = err.Error()
		createKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	if err = json.Unmarshal(writeMapBytes, &writeMap); err != nil {
		r.logger.Errorf("get write map failed, %s", err.Error())
		createKeyHistoryIterResponse.SysCallMessage.Message = err.Error()
		createKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	gasUsed, err = r.mergeSimContextWriteMap(txSimContext, writeMap, gasUsed, contractName)
	if err != nil {
		r.logger.Errorf("merge the sim context write map failed, %s", err.Error())
		createKeyHistoryIterResponse.SysCallMessage.Message = err.Error()
		createKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	if err = protocol.CheckKeyFieldStr(string(keyStr), string(field)); err != nil {
		r.logger.Errorf("invalid key field str, %s", err.Error())
		createKeyHistoryIterResponse.SysCallMessage.Message = err.Error()
		createKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	key := protocol.GetKeyStr(string(keyStr), string(field))

	iter, err := txSimContext.GetHistoryIterForKey(string(calledContractName), key)
	if err != nil {
		createKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.SysCallMessage.Message = err.Error()
		createKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	index := atomic.AddInt32(&r.rowIndex, 1)
	txSimContext.SetIterHandle(index, iter)

	r.logger.Debug("create key history iterator: ", index)

	createKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	createKeyHistoryIterResponse.SysCallMessage.Payload = map[string][]byte{
		config.KeyIterIndex: bytehelper.IntToBytes(index),
	}

	return createKeyHistoryIterResponse, gasUsed
}

func (r *RuntimeInstance) handleConsumeKeyHistoryIterator(txId string, recvMsg *protogo.DockerVMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.DockerVMMessage, uint64) {
	consumeKeyHistoryIterResponse := r.newEmptyResponse(txId, protogo.DockerVMType_CONSUME_KEY_HISTORY_ITER_RESPONSE)
	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	// for compatible with version 2030102
	var currentGasUsed uint64
	var err error
	if blockVersion < blockVersion2312 {
		currentGasUsed, err = gas.ConsumeKvIteratorGasUsedLt2312(gasUsed)
		if err != nil {
			consumeKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			consumeKeyHistoryIterResponse.SysCallMessage.Message = err.Error()
			consumeKeyHistoryIterResponse.SysCallMessage.Payload = nil
			return consumeKeyHistoryIterResponse, currentGasUsed
		}
	}

	/*
		|	index	|			desc				|
		|	----	|			----  				|
		|	 0  	|	consumeKvIteratorFunc		|
		|	 1  	|		rsIndex					|
	*/

	consumeKeyHistoryIteratorFunc := recvMsg.SysCallMessage.Payload[config.KeyIteratorFuncName]
	keyHistoryIterIndex, err := bytehelper.BytesToInt(recvMsg.SysCallMessage.Payload[config.KeyIterIndex])
	if err != nil {
		r.logger.Errorf("failed to get iterator index, %s", err.Error())
		consumeKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.SysCallMessage.Message = err.Error()
		consumeKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}

	iter, ok := txSimContext.GetIterHandle(keyHistoryIterIndex)
	if !ok {
		errMsg := fmt.Sprintf("[key history iterator consume] can not found iterator index [%d]", keyHistoryIterIndex)
		r.logger.Error(errMsg)

		consumeKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.SysCallMessage.Message = errMsg
		consumeKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}

	keyHistoryIterator, ok := iter.(protocol.KeyHistoryIterator)
	if !ok {
		errMsg := "assertion failed"
		r.logger.Error(errMsg)

		consumeKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.SysCallMessage.Message = errMsg
		consumeKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}

	switch string(consumeKeyHistoryIteratorFunc) {
	case config.FuncKeyHistoryIterHasNext:
		return keyHistoryIterHasNext(blockVersion, gasConfig, keyHistoryIterator, gasUsed, consumeKeyHistoryIterResponse)

	case config.FuncKeyHistoryIterNext:
		return keyHistoryIterNext(blockVersion, gasConfig, keyHistoryIterator, gasUsed, consumeKeyHistoryIterResponse)

	case config.FuncKeyHistoryIterClose:
		return keyHistoryIterClose(blockVersion, gasConfig, keyHistoryIterator, gasUsed, consumeKeyHistoryIterResponse)
	default:
		consumeKeyHistoryIterResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.SysCallMessage.Message = fmt.Sprintf("%s not found", consumeKeyHistoryIteratorFunc)
		consumeKeyHistoryIterResponse.SysCallMessage.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}
}

func (r *RuntimeInstance) handleGetSenderAddress(txId string,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.DockerVMMessage, uint64) {
	getSenderAddressResponse := r.newEmptyResponse(txId, protogo.DockerVMType_GET_SENDER_ADDRESS_RESPONSE)

	var err error
	gasUsed, err = gas.GetSenderAddressGasUsed(gasUsed)
	if err != nil {
		getSenderAddressResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		getSenderAddressResponse.SysCallMessage.Message = err.Error()
		getSenderAddressResponse.SysCallMessage.Payload = nil
		return getSenderAddressResponse, gasUsed
	}

	var address string
	address, err = txSimContext.GetStrAddrFromPbMember(txSimContext.GetSender())
	if err != nil {
		r.logger.Error(err.Error())
		getSenderAddressResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		getSenderAddressResponse.SysCallMessage.Message = err.Error()
		getSenderAddressResponse.SysCallMessage.Payload = nil
		return getSenderAddressResponse, gasUsed
	}

	r.logger.Debug("get sender address: ", address)
	getSenderAddressResponse.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	getSenderAddressResponse.SysCallMessage.Payload = map[string][]byte{
		config.KeySenderAddr: []byte(address),
	}

	return getSenderAddressResponse, gasUsed
}

func kvIteratorCreate(txSimContext protocol.TxSimContext,
	gasConfig *gasutils.GasConfig, calledContractName string,
	params map[string][]byte, gasUsed uint64, txId string, log protocol.Logger) (protocol.StateIterator, uint64, error) {
	var err error
	blockVersion := txSimContext.GetBlockVersion()
	startKey := string(params[config.KeyIterStartKey])
	startField := string(params[config.KeyIterStartField])
	limitKey := string(params[config.KeyIterLimitKey])
	limitField := string(params[config.KeyIterLimitField])

	gasUsed, err = gas.CreateKvIteratorGasUsed(blockVersion, gasConfig,
		params, gasUsed, txId, log)
	if err != nil {
		log.Errorf("failed to create kv iterator, %s", err.Error())
		return nil, gasUsed, err
	}

	key := protocol.GetKeyStr(startKey, startField)
	if err = protocol.CheckKeyFieldStr(limitKey, limitField); err != nil {
		return nil, gasUsed, err
	}
	limit := protocol.GetKeyStr(limitKey, limitField)
	var iter protocol.StateIterator
	iter, err = txSimContext.Select(calledContractName, key, limit)
	if err != nil {
		return nil, gasUsed, err
	}

	return iter, gasUsed, err
}

func kvIteratorClose(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	kvIterator protocol.StateIterator, gasUsed uint64,
	response *protogo.DockerVMMessage) (*protogo.DockerVMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKvIteratorGasUsed(blockVersion, gasConfig, gasUsed)
	if err != nil {
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Payload = nil
		return response, gasUsed
	}

	kvIterator.Release()
	response.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	response.SysCallMessage.Payload = nil

	return response, gasUsed
}

func (r *RuntimeInstance) mergeSimContextWriteMap(txSimContext protocol.TxSimContext,
	writeMap map[string][]byte, gasUsed uint64, txContractName string) (uint64, error) {

	blockVersion := txSimContext.GetBlockVersion()
	gasConfig := gasutils.NewGasConfig(txSimContext.GetLastChainConfig().AccountConfig)

	// merge the sim context write map
	for key, value := range writeMap {
		var contractName string
		var contractKey string
		var contractField string
		keyList := strings.Split(key, "#")
		keyLen := len(keyList)
		if keyLen < 2 {
			return gasUsed, fmt.Errorf("key list length == %d, needs to be >= 2", keyLen)
		}
		contractName = keyList[0]
		if contractName != txContractName {
			return gasUsed, fmt.Errorf("wrong contract name [%s] of write map key, need [%s]",
				contractName, txContractName)
		}
		contractKey = keyList[1]
		if keyLen == 3 {
			contractField = keyList[2]
		}
		// put state gas used calc and check gas limit
		var err error
		gasUsed, err = gas.PutStateGasUsed(blockVersion, gasConfig, gasUsed, contractName, contractKey, contractField, value)
		r.logger.Debugf("【gas calc】%v, merge write set, gasUsed = %v => %v # %v # %v = data(%v)",
			txSimContext.GetTx().Payload.TxId, gasUsed, contractName, contractKey, contractField, len(value))
		if err != nil {
			return gasUsed, err
		}

		err = txSimContext.Put(contractName, protocol.GetKeyStr(contractKey, contractField), value)
		if err != nil {
			return gasUsed, err
		}
	}

	return gasUsed, nil
}

func kvIteratorHasNext(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	kvIterator protocol.StateIterator, gasUsed uint64,
	response *protogo.DockerVMMessage) (*protogo.DockerVMMessage, uint64) {

	var err error
	gasUsed, err = gas.ConsumeKvIteratorGasUsed(blockVersion, gasConfig, gasUsed)
	if err != nil {
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Payload = nil
		return response, gasUsed
	}

	hasNext := config.BoolFalse //nolint:typecheck
	if kvIterator.Next() {
		hasNext = config.BoolTrue
	}

	response.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	response.SysCallMessage.Payload = map[string][]byte{
		config.KeyIteratorHasNext: bytehelper.IntToBytes(int32(hasNext)),
	}

	return response, gasUsed
}

func kvIteratorNext(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	kvIterator protocol.StateIterator, gasUsed uint64,
	response *protogo.DockerVMMessage) (*protogo.DockerVMMessage, uint64) {

	var err error
	if blockVersion < blockVersion2312 {
		gasUsed, err = gas.ConsumeKvIteratorGasUsedLt2312(gasUsed)
		if err != nil {
			response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			response.SysCallMessage.Message = err.Error()
			response.SysCallMessage.Payload = nil
			return response, gasUsed
		}
	}

	if kvIterator == nil {
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		response.SysCallMessage.Message = msgIterIsNil
		response.SysCallMessage.Payload = nil
		return response, gasUsed
	}

	var kvRow *store.KV
	kvRow, err = kvIterator.Value()
	if err != nil {
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Payload = nil
		return response, gasUsed
	}

	arrKey := strings.Split(string(kvRow.Key), "#")
	key := arrKey[0]
	field := ""
	if len(arrKey) > 1 {
		field = arrKey[1]
	}

	value := kvRow.Value

	response.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	response.SysCallMessage.Payload = map[string][]byte{
		config.KeyUserKey:    []byte(key),
		config.KeyUserField:  []byte(field),
		config.KeyStateValue: value,
	}

	if blockVersion2312 <= blockVersion {
		gasUsed, err = gas.ConsumeKvIteratorNextGasUsed2312(gasConfig, gasUsed, key, field, value)
		if err != nil {
			response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			response.SysCallMessage.Message = err.Error()
			response.SysCallMessage.Payload = nil
			return response, gasUsed
		}
	}

	return response, gasUsed
}

func keyHistoryIterHasNext(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	iter protocol.KeyHistoryIterator, gasUsed uint64,
	response *protogo.DockerVMMessage) (*protogo.DockerVMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKeyHistoryIterGasUsed(blockVersion, gasConfig, gasUsed)
	if err != nil {
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Payload = nil
		return response, gasUsed
	}

	hasNext := config.BoolFalse //nolint:typecheck
	if iter.Next() {
		hasNext = config.BoolTrue
	}

	response.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	response.SysCallMessage.Payload = map[string][]byte{
		config.KeyIteratorHasNext: bytehelper.IntToBytes(int32(hasNext)),
	}

	return response, gasUsed
}

func keyHistoryIterNext(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	iter protocol.KeyHistoryIterator, gasUsed uint64,
	response *protogo.DockerVMMessage) (*protogo.DockerVMMessage, uint64) {
	var err error
	if blockVersion < blockVersion2312 {
		gasUsed, err = gas.ConsumeKeyHistoryIterGasUsedLt2312(gasUsed)
		if err != nil {
			response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			response.SysCallMessage.Message = err.Error()
			response.SysCallMessage.Payload = nil
			return response, gasUsed
		}
	}

	if iter == nil {
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		response.SysCallMessage.Message = msgIterIsNil
		response.SysCallMessage.Payload = nil
		return response, gasUsed
	}

	var historyValue *store.KeyModification
	historyValue, err = iter.Value()
	if err != nil {
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Payload = nil
		return response, gasUsed
	}

	response.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess

	if blockVersion2312 <= blockVersion {
		gasUsed, err = gas.ConsumeKeyHistoryIterNextGasUsed2312(gasConfig, gasUsed, historyValue.Value)
		if err != nil {
			response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
			response.SysCallMessage.Message = err.Error()
			response.SysCallMessage.Payload = nil
			return response, gasUsed
		}
	}
	/*
		| index | desc        |
		| ---   | ---         |
		| 0     | txId        |
		| 1     | blockHeight |
		| 2     | value       |
		| 3     | isDelete    |
		| 4     | timestamp   |
	*/
	blockHeight := bytehelper.IntToBytes(int32(historyValue.BlockHeight)) //nolint:gosec,typecheck
	timestampStr := strconv.FormatInt(historyValue.Timestamp, 10)         //nolint:typecheck
	isDelete := config.BoolTrue                                           //nolint:typecheck
	if !historyValue.IsDelete {
		isDelete = config.BoolFalse
	}
	response.SysCallMessage.Payload = map[string][]byte{
		config.KeyTxId:        []byte(historyValue.TxId),
		config.KeyBlockHeight: blockHeight,
		config.KeyStateValue:  historyValue.Value,
		config.KeyIsDelete:    bytehelper.IntToBytes(int32(isDelete)),
		config.KeyTimestamp:   []byte(timestampStr),
	}

	return response, gasUsed
}

func keyHistoryIterClose(
	blockVersion uint32, gasConfig *gasutils.GasConfig,
	iter protocol.KeyHistoryIterator, gasUsed uint64,
	response *protogo.DockerVMMessage) (*protogo.DockerVMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKeyHistoryIterGasUsed(blockVersion, gasConfig, gasUsed)
	if err != nil {
		response.SysCallMessage.Code = protocol.ContractSdkSignalResultFail
		response.SysCallMessage.Message = err.Error()
		response.SysCallMessage.Payload = nil
		return response, gasUsed
	}

	iter.Release()
	response.SysCallMessage.Code = protocol.ContractSdkSignalResultSuccess
	response.SysCallMessage.Payload = nil

	return response, gasUsed
}

func (r *RuntimeInstance) handleGetByteCodeRequest(
	txId string,
	txSimContext protocol.TxSimContext,
	recvMsg *protogo.DockerVMMessage,
	byteCode []byte, contractIndex uint32) *protogo.DockerVMMessage {

	response := &protogo.DockerVMMessage{
		ChainId: r.chainId,
		TxId:    txId,
		Type:    protogo.DockerVMType_GET_BYTECODE_RESPONSE,
		Response: &protogo.TxResponse{
			ChainId:         r.chainId,
			Result:          make([]byte, 1),
			ContractName:    recvMsg.Request.ContractName,
			ContractVersion: recvMsg.Request.ContractVersion,
			ContractIndex:   contractIndex,
			Code:            protogo.DockerVMCode_FAIL,
		},
	}

	contractFullName := constructContractFileName(recvMsg.Request.ChainId,
		recvMsg.Request.ContractName,
		recvMsg.Request.ContractVersion,
		contractIndex)
	contractName := recvMsg.Request.ContractName
	r.logger.DebugDynamic(func() string {
		return fmt.Sprintf("contract name: %s, contract full name: %s", contractName, contractFullName)
	})

	sendContract := r.clientMgr.NeedSendContractByteCode()

	// bytecode == 0
	// 		get 7z -> save 7z to chainID#contractName#Version -> extract
	//			uds: return path, mv bin .. , remove dir
	//			tcp: return bin, remove dir
	// bytecode != 0
	//		get bytecode -> save bytecode to chainID#contractName#Version
	// 			uds: return path, mv bin ..
	//			tcp: return bin, remove dir

	var err error
	if len(byteCode) == 0 {
		r.logger.Warnf("[%s] bytecode is missing", txId)

		// get bytecode 7z from txSimContext / database
		startTime := time.Now()
		byteCode, err = txSimContext.GetContractBytecode(contractName)
		if err != nil || len(byteCode) == 0 {
			r.currSysCall.AddStorageDuration(time.Since(startTime).Nanoseconds())

			r.logger.Errorf("[%s] fail to get contract bytecode: %s, required contract name is: [%s]", txId, err,
				contractName)
			if err != nil {
				response.Response.Message = err.Error()
			} else {
				response.Response.Message = "contract byte is nil"
			}
			return response
		}

		r.logger.Infof("[%s] get contract bytecode [%s] from chain db succeed", txId, contractFullName)

		r.currSysCall.AddStorageDuration(time.Since(startTime).Nanoseconds())

		// got extracted bytecode
		var extractedBytecode []byte
		extractedBytecode, err = r.extractContract(byteCode, contractFullName, sendContract)
		if err != nil {
			r.logger.Errorf("[%s] failed to extract contract %s from tx_sim_context, %v",
				txId, contractName, err)
			response.Response.Message = err.Error()
			return response
		}

		r.logger.Infof("[%s] extract contract bytecode [%s] succeed", txId, contractFullName)

		// tcp: need to send bytecode
		if sendContract {
			response.Response.Result = extractedBytecode
		}
		response.Response.Code = protogo.DockerVMCode_OK
		return response
	}

	// uds: need to save bytecode
	if !sendContract {
		hostMountPath := r.clientMgr.GetVMConfig().DockerVMMountPath
		contractDir := filepath.Join(hostMountPath, mountContractDir)
		contractFullNamePath := filepath.Join(contractDir, contractFullName)

		// save bytecode to disk
		err = r.saveBytesToDisk(byteCode, contractFullNamePath, "")
		if err != nil {
			r.logger.Errorf("failed to save bytecode to disk: %s", err)
			response.Response.Message = err.Error()
		}
		response.Response.Code = protogo.DockerVMCode_OK
		return response
	}

	// tcp: need to send bytecode
	response.Response.Code = protogo.DockerVMCode_OK
	response.Response.Result = byteCode

	return response
}

func (r *RuntimeInstance) errorResult(
	contractResult *commonPb.ContractResult,
	err error,
	errMsg string) (*commonPb.ContractResult, protocol.ExecOrderTxType) {
	contractResult.Code = uint32(1)
	//if err != nil {
	//	errMsg += ", " + err.Error()
	//}
	contractResult.Message = errMsg
	//r.logger.Error(errMsg)
	return contractResult, protocol.ExecOrderTxTypeNormal
}

// saveBytesToDisk save contract bytecode to disk
func (r *RuntimeInstance) saveBytesToDisk(bytes []byte, newFilePath, newFileDir string) error {

	if len(newFileDir) != 0 {
		if _, err := os.Stat(newFilePath); os.IsNotExist(err) {
			err = os.Mkdir(newFileDir, 0777)
			if err != nil {
				return err
			}
		}
	}

	f, err := os.OpenFile(newFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err = f.Close()
		if err != nil {
			return
		}
	}(f)

	_, err = f.Write(bytes)
	if err != nil {
		return err
	}

	return f.Sync()
}

// RunCmd exec cmd
//func (r *RuntimeInstance) runCmd(command string) error {
//	var stderr bytes.Buffer
//	commands := strings.Split(command, " ")
//	cmd := exec.Command(commands[0], commands[1:]...) // #nosec
//	cmd.Stderr = &stderr
//
//	if err := cmd.Start(); err != nil {
//		r.logger.Errorf("failed to run cmd %s start, %v, %v", command, err, stderr.String())
//		return err
//	}
//
//	if err := cmd.Wait(); err != nil {
//		r.logger.Errorf("failed to run cmd %s wait, %v, %v", command, err, stderr.String())
//		return err
//	}
//	return nil
//}

func (r *RuntimeInstance) newEmptyResponse(txId string, msgType protogo.DockerVMType) *protogo.DockerVMMessage {
	return &protogo.DockerVMMessage{
		ChainId: r.chainId,
		TxId:    txId,
		Type:    msgType,
		SysCallMessage: &protogo.SysCallMessage{
			Payload: map[string][]byte{},
			Message: "",
		},
		Response: nil,
		Request:  nil,
	}
}

// extractContract extract contract from 7z to bin
func (r *RuntimeInstance) extractContract(bytecode []byte, contractFullName string, sendContract bool) ([]byte, error) {

	// tmp contract dir (include .7z and bin files)
	tmpContractDir := "tmp-contract-" + uuid.New().String()

	// contract zip path (.7z path)
	contractZipPath := filepath.Join(tmpContractDir, fmt.Sprintf("%s.7z", contractFullName))

	// save bytecode to tmpContractDir
	var err error
	err = r.saveBytesToDisk(bytecode, contractZipPath, tmpContractDir)
	if err != nil {
		return nil, fmt.Errorf("failed to save tmp contract bin for version query")
	}

	// extract 7z file
	unzipCommand := fmt.Sprintf("7z e %s -o%s -y", contractZipPath, tmpContractDir) // contract1
	err = runCmd(unzipCommand)
	if err != nil {
		return nil, fmt.Errorf("failed to extract contract, %v", err)
	}

	// remove tmpContractDir in the end
	defer func() {
		if err = os.RemoveAll(tmpContractDir); err != nil {
			r.logger.Errorf("failed to remove tmp contract bin dir %s", tmpContractDir)
		}
	}()

	// exec contract bin to get version
	// read all files in tmpContractDir
	fileInfoList, err := ioutil.ReadDir(tmpContractDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read tmp contract dir %s, %v", tmpContractDir, err)
	}

	// 2 files, include .7z file and bin file
	if len(fileInfoList) != 2 {
		return nil, fmt.Errorf("file num in contract dir %s != 2", tmpContractDir)
	}

	// range file list
	for i := range fileInfoList {

		// skip .7z file
		if strings.HasSuffix(fileInfoList[i].Name(), ".7z") {
			continue
		}

		r.logger.Debugf("found file [%s] [size: %d] while extract contract [%s]",
			fileInfoList[i].Name(), fileInfoList[i].Size(), contractFullName)

		// skip .7z file

		// get contract bin file path
		fp := filepath.Join(tmpContractDir, fileInfoList[i].Name())
		extBytecode, err := ioutil.ReadFile(fp)
		if err != nil {
			return nil, fmt.Errorf("read from byteCode file %s failed, %s", fp, err)
		}

		// uds, need to save bytecode
		if !sendContract {
			hostMountPath := r.clientMgr.GetVMConfig().DockerVMMountPath
			contractDir := filepath.Join(hostMountPath, mountContractDir)
			contractFullNamePath := filepath.Join(contractDir, contractFullName)
			err = r.saveBytesToDisk(extBytecode, contractFullNamePath, "")
			if err != nil {
				return nil, fmt.Errorf("failed to save bytecode to disk: %s", err)
			}
		}
		return extBytecode, nil
	}

	return nil, fmt.Errorf("no contract binaries satisfied")
}

// constructContractKey chainId#contractName#contractVersion
func constructContractKey(keys ...string) string {
	return strings.Join(keys, "#")
}

// ConstructContractFileName chainID#contractName#contractVersion#index
func constructContractFileName(chainId, contractName, contractVersion string, index uint32) string {
	// 这里意味着 block version 是2.3.5之前的版本
	if index == 0 {
		return constructContractKey(chainId, contractName, contractVersion)
	}
	// 大于 2.3.5 版本之后，合约版本会带上 index。不再只依赖用户定义的版本号做唯一标识
	return constructContractKey(chainId, contractName, contractVersion, strconv.FormatUint(uint64(index), 10))
}

// runCmd exec cmd
func runCmd(command string) error {
	var stderr bytes.Buffer
	commands := strings.Split(command, " ")
	cmd := exec.Command(commands[0], commands[1:]...) // #nosec
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to run cmd %s start, %v, %v", command, err, stderr.String())
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("failed to run cmd %s wait, %v, %v", command, err, stderr.String())
	}
	return nil
}
