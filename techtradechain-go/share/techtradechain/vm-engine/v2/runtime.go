/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package docker_go

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	protocol "techtradechain.com/techtradechain/protocol/v2"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/vm-engine/v2/config"
	"techtradechain.com/techtradechain/vm-engine/v2/gas"
	"techtradechain.com/techtradechain/vm-engine/v2/interfaces"
	"techtradechain.com/techtradechain/vm-engine/v2/pb/protogo"
	"techtradechain.com/techtradechain/vm-engine/v2/utils"
)

const (
	mountContractDir        = "contract-bins"
	msgIterIsNil            = "iterator is nil"
	version2310      uint32 = 2030100
	version2312      uint32 = 2030102
	version233       uint32 = 2030300
	version235       uint32 = 2030500
	version300       uint32 = 3000000
)

var dockerVMMsgPool = sync.Pool{
	New: func() interface{} {
		return &protogo.DockerVMMessage{
			Request: &protogo.TxRequest{
				TxContext: &protogo.TxContext{
					WriteMap: nil,
					ReadMap:  nil,
				},
			},
			CrossContext:  &protogo.CrossContext{},
			StepDurations: make([]*protogo.StepDuration, 0, 4),
		}
	},
}

// RuntimeInstance docker-go runtime
type RuntimeInstance struct {
	rowIndex        int32                              // iterator index
	chainId         string                             // chain id
	logger          protocol.Logger                    //
	sendSysResponse func(msg *protogo.DockerVMMessage) //
	event           []*commonPb.ContractEvent          // tx event cache
	clientMgr       interfaces.ContractEngineClientMgr //
	runtimeService  interfaces.RuntimeService          //

	sandboxMsgCh        chan *protogo.DockerVMMessage
	contractEngineMsgCh chan *protogo.DockerVMMessage
	DockerManager       *InstancesManager
	//txDuration          *utils.TxDuration
	currSysCall *utils.SysCallDuration
}

func (r *RuntimeInstance) contractEngineMsgNotify(msg *protogo.DockerVMMessage) {
	r.contractEngineMsgCh <- msg
}

func (r *RuntimeInstance) sandboxMsgNotify(msg *protogo.DockerVMMessage, sendF func(msg *protogo.DockerVMMessage)) {
	r.sendSysResponse = sendF
	r.sandboxMsgCh <- msg
}

// Invoke process one tx in docker and return result
// nolint: gocyclo, revive
func (r *RuntimeInstance) Invoke(
	contract *commonPb.Contract,
	method string,
	byteCode []byte,
	parameters map[string][]byte,
	txSimContext protocol.TxSimContext,
	gasUsed uint64,
) (contractResult *commonPb.ContractResult, execOrderTxType protocol.ExecOrderTxType) {

	originalTxId := txSimContext.GetTx().Payload.TxId
	uniqueTxKey := r.clientMgr.GetUniqueTxKey(originalTxId)
	r.logger.DebugDynamic(func() string {
		return fmt.Sprintf("start handling tx [%s]", originalTxId)
	})

	// contract response
	contractResult = &commonPb.ContractResult{
		// TODO
		Code:    uint32(1),
		Result:  nil,
		Message: "",
	}

	if !r.clientMgr.HasActiveConnections() {
		r.logger.Errorf("contract engine client stream not ready, waiting reconnect, tx id: %s", originalTxId)
		err := errors.New("contract engine client not connected")
		return r.errorResult(contractResult, err, err.Error())
	}

	specialTxType := protocol.ExecOrderTxTypeNormal

	r.logger.Debugf("【gas calc】%v, before vm-engine calc gas => gasUsed = %v, blockVersion = %v",
		txSimContext.GetTx().Payload.TxId, gasUsed, txSimContext.GetBlockVersion())
	var err error
	if txSimContext.GetBlockVersion() < version2312 {
		// init func gas used calc and check gas limit
		if gasUsed, err = gas.InitFuncGasUsedLt2312(gasUsed, r.getChainConfigDefaultGas(txSimContext)); err != nil {
			contractResult.GasUsed = gasUsed
			return r.errorResult(contractResult, err, err.Error())
		}

		//init contract gas used calc and check gas limit
		gasUsed, err = gas.ContractGasUsed(gasUsed, method, contract.Name, byteCode)
		if err != nil {
			contractResult.GasUsed = gasUsed
			return r.errorResult(contractResult, err, err.Error())
		}
	}

	r.logger.Debugf("【gas calc】%v, after vm-engine calc gas => gasUsed = %v",
		txSimContext.GetTx().Payload.TxId, gasUsed)

	for key := range parameters {
		if strings.Contains(key, "CONTRACT") {
			delete(parameters, key)
		}
	}

	dockerVMMsg, ok := dockerVMMsgPool.Get().(*protogo.DockerVMMessage)
	if !ok && txSimContext.GetBlockVersion() >= version235 {
		contractResult.Message = "convert docker vm msg failed."
		return contractResult, specialTxType
	}
	dockerVMMsg.ChainId = r.chainId
	dockerVMMsg.TxId = uniqueTxKey
	dockerVMMsg.Type = protogo.DockerVMType_TX_REQUEST
	dockerVMMsg.Request.ChainId = r.chainId
	dockerVMMsg.Request.ContractName = contract.Name
	dockerVMMsg.Request.ContractVersion = contract.Version
	dockerVMMsg.Request.ContractIndex = contract.Index
	dockerVMMsg.Request.Method = method
	dockerVMMsg.Request.Parameters = parameters
	if txSimContext.GetBlockVersion() >= version233 && txSimContext.GetBlockVersion() < version300 {
		// 如果是至信链的地址，需要添加前缀，CM和EVM地址不需要添加前缀
		address := contract.Address
		chainConfig := txSimContext.GetLastChainConfig()
		if chainConfig.Vm.AddrType == configPb.AddrType_ZXL {
			address = "ZX" + address
		}
		dockerVMMsg.Request.ContractAddr = address
	}
	dockerVMMsg.CrossContext.CrossInfo = txSimContext.GetCrossInfo()
	dockerVMMsg.CrossContext.CurrentDepth = uint32(txSimContext.GetDepth()) //nolint:gosec
	dockerVMMsg.StepDurations = make([]*protogo.StepDuration, 0, 4)
	defer func() {
		dockerVMMsgPool.Put(dockerVMMsg)
		//for _, dur := range dockerVMMsg.StepDurations {
		//	utils.TxStepPool.Put(dur)
		//}
	}()

	utils.EnterNextStep(dockerVMMsg, protogo.StepType_RUNTIME_PREPARE_TX_REQUEST, func() string {
		return strings.Join([]string{"pos", strconv.Itoa(r.clientMgr.GetTxSendChLen())}, ":")
	})

	// init time statistics
	startTime := time.Now()
	txDuration := utils.NewTxDuration(originalTxId, uniqueTxKey, startTime.UnixNano())

	// add time statistics
	fingerprint := txSimContext.GetBlockFingerprint()
	r.addTxDuration(txSimContext, fingerprint, txDuration)

	defer func() {
		txDuration.EndTime = time.Now().UnixNano()
		txDuration.TotalDuration = time.Since(startTime).Nanoseconds()
		r.DockerManager.BlockDurationMgr.FinishTx(fingerprint, txDuration)
		//r.logger.Debugf(txDuration.PrintSysCallList())
	}()

	// register notify for sandbox msg
	err = r.runtimeService.RegisterSandboxMsgNotify(r.chainId, uniqueTxKey, r.sandboxMsgNotify)
	if err != nil {
		return r.errorResult(contractResult, err, err.Error())
	}

	// register receive notify
	err = r.clientMgr.PutTxRequestWithNotify(dockerVMMsg, r.chainId, r.contractEngineMsgNotify)
	if err != nil {
		return r.errorResult(contractResult, err, err.Error())
	}

	// send message to tx chan
	r.logger.DebugDynamic(func() string {
		return fmt.Sprintf("[%s] put tx in send chan with length [%d]", dockerVMMsg.TxId, r.clientMgr.GetTxSendChLen())
	})

	defer func() {
		_ = r.runtimeService.DeleteSandboxMsgNotify(r.chainId, uniqueTxKey)
		_ = r.clientMgr.DeleteNotify(r.chainId, uniqueTxKey)
	}()

	timeoutC := time.After(time.Duration(config.VMConfig.TxTimeout) * time.Second)

	// wait this chan
	for {
		select {
		case recvMsg := <-r.contractEngineMsgCh:

			r.currSysCall = txDuration.StartSysCall(recvMsg.Type)

			switch recvMsg.Type {
			case protogo.DockerVMType_GET_BYTECODE_REQUEST:
				r.logger.Debugf("tx [%s] start get bytecode", uniqueTxKey)
				getByteCodeResponse := r.handleGetByteCodeRequest(uniqueTxKey, txSimContext, recvMsg, byteCode, contract.Index)
				r.clientMgr.PutByteCodeResp(getByteCodeResponse)
				r.logger.Debugf("tx [%s] finish get bytecode", uniqueTxKey)
				if err = txDuration.EndSysCall(recvMsg); err != nil {
					r.logger.Warnf("failed to end syscall, %v", err)
				}

			case protogo.DockerVMType_ERROR:
				r.logger.Warnf("handle tx [%s] failed, err: [%s]", originalTxId, recvMsg.Response.Message)
				contractResult.GasUsed = gasUsed
				return r.errorResult(
					contractResult,
					fmt.Errorf("tx timeout"),
					recvMsg.Response.Message,
				)

			default:
				contractResult.GasUsed = gasUsed
				return r.errorResult(
					contractResult,
					fmt.Errorf("unknown msg type"),
					"unknown msg type",
				)
			}

			// TODO: 超时时间自定义
		case <-timeoutC:
			r.logger.Errorf(
				"handle tx [%s] failed, fail to receive response in %d secs and return timeout response, %s",
				originalTxId, config.VMConfig.TxTimeout, utils.PrintTxSteps(dockerVMMsg))
			r.logger.Infof(txDuration.ToString())
			r.logger.InfoDynamic(func() string {
				return txDuration.PrintSysCallList()
			})
			contractResult.GasUsed = gasUsed
			return r.errorResult(
				contractResult,
				fmt.Errorf("tx timeout"),
				"tx timeout",
			)

		case recvMsg := <-r.sandboxMsgCh:

			r.currSysCall = txDuration.StartSysCall(recvMsg.Type)

			switch recvMsg.Type {
			case protogo.DockerVMType_GET_STATE_REQUEST:
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] start get state", uniqueTxKey)
				})
				var getStateResponse *protogo.DockerVMMessage
				getStateResponse, gasUsed = r.handleGetStateRequest(uniqueTxKey, recvMsg, txSimContext, gasUsed)

				r.sendSysResponse(getStateResponse)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish get state", uniqueTxKey)
				})

			case protogo.DockerVMType_GET_BATCH_STATE_REQUEST:
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] start get batch state", uniqueTxKey)
				})
				if txSimContext.GetBlockVersion() < version2310 {
					r.logger.Errorf("get batch state is forbidden on version < v2.3.1, will be timeout")
					break
				}
				var getStateResponse *protogo.DockerVMMessage
				getStateResponse, gasUsed = r.handleGetBatchStateRequest(uniqueTxKey, recvMsg, txSimContext, gasUsed)

				r.sendSysResponse(getStateResponse)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish get batch state", uniqueTxKey)
				})

			case protogo.DockerVMType_TX_RESPONSE:
				result, txType := r.handleTxResponse(originalTxId, recvMsg, txSimContext,
					gasUsed, specialTxType, contract.Name)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish handle response", originalTxId)
				})
				if err = txDuration.EndSysCall(recvMsg); err != nil {
					r.logger.Warnf("failed to end syscall, %v", err)
				}
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] do some work after receive response", originalTxId)
				})
				utils.ReturnToPool(recvMsg)
				return result, txType

			case protogo.DockerVMType_CALL_CONTRACT_REQUEST:
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] start call contract", uniqueTxKey)
				})
				var callContractResponse *protogo.DockerVMMessage
				var crossTxType protocol.ExecOrderTxType
				callContractResponse, gasUsed, crossTxType = r.handlerCallContract(
					uniqueTxKey,
					recvMsg,
					txSimContext,
					gasUsed,
					contract,
				)
				if crossTxType != protocol.ExecOrderTxTypeNormal {
					specialTxType = crossTxType
				}
				r.sendSysResponse(callContractResponse)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish call contract", uniqueTxKey)
				})

			case protogo.DockerVMType_CREATE_KV_ITERATOR_REQUEST:
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] start create kv iterator", uniqueTxKey)
				})
				var createKvIteratorResponse *protogo.DockerVMMessage
				specialTxType = protocol.ExecOrderTxTypeIterator
				createKvIteratorResponse, gasUsed = r.handleCreateKvIterator(uniqueTxKey, recvMsg, txSimContext,
					gasUsed, contract.Name)

				r.sendSysResponse(createKvIteratorResponse)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish create kv iterator", uniqueTxKey)
				})

			case protogo.DockerVMType_CONSUME_KV_ITERATOR_REQUEST:
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] start consume kv iterator", uniqueTxKey)
				})
				var consumeKvIteratorResponse *protogo.DockerVMMessage
				specialTxType = protocol.ExecOrderTxTypeIterator
				consumeKvIteratorResponse, gasUsed = r.handleConsumeKvIterator(uniqueTxKey, recvMsg, txSimContext, gasUsed)

				r.sendSysResponse(consumeKvIteratorResponse)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish consume kv iterator", uniqueTxKey)
				})

			case protogo.DockerVMType_CREATE_KEY_HISTORY_ITER_REQUEST:
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] start create key history iterator", uniqueTxKey)
				})
				var createKeyHistoryIterResp *protogo.DockerVMMessage
				specialTxType = protocol.ExecOrderTxTypeIterator
				createKeyHistoryIterResp, gasUsed = r.handleCreateKeyHistoryIterator(uniqueTxKey, recvMsg,
					txSimContext, gasUsed, contract.Name)
				r.sendSysResponse(createKeyHistoryIterResp)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish create key history iterator", uniqueTxKey)
				})

			case protogo.DockerVMType_CONSUME_KEY_HISTORY_ITER_REQUEST:
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] start consume key history iterator", uniqueTxKey)
				})
				var consumeKeyHistoryResp *protogo.DockerVMMessage
				specialTxType = protocol.ExecOrderTxTypeIterator
				consumeKeyHistoryResp, gasUsed = r.handleConsumeKeyHistoryIterator(uniqueTxKey, recvMsg, txSimContext, gasUsed)
				r.sendSysResponse(consumeKeyHistoryResp)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish consume key history iterator", uniqueTxKey)
				})

			case protogo.DockerVMType_GET_SENDER_ADDRESS_REQUEST:
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] start get sender address", uniqueTxKey)
				})
				var getSenderAddressResp *protogo.DockerVMMessage
				getSenderAddressResp, gasUsed = r.handleGetSenderAddress(uniqueTxKey, txSimContext, gasUsed)
				r.sendSysResponse(getSenderAddressResp)
				r.logger.DebugDynamic(func() string {
					return fmt.Sprintf("tx [%s] finish get sender address", uniqueTxKey)
				})

			default:
				contractResult.GasUsed = gasUsed
				return r.errorResult(
					contractResult,
					fmt.Errorf("unknow msg type"),
					"unknown msg type",
				)
			}
			if err = txDuration.EndSysCall(recvMsg); err != nil {
				r.logger.Warnf("failed to end syscall, %v", err)
			}
		}
	}
}

func (r *RuntimeInstance) getChainConfigDefaultGas(txSimContext protocol.TxSimContext) uint64 {
	chainConfig := txSimContext.GetLastChainConfig()
	if chainConfig.AccountConfig != nil && chainConfig.AccountConfig.DefaultGas > 0 {
		return chainConfig.AccountConfig.DefaultGas
	}
	r.logger.Debug("account config not set default gas value")
	return 0
}

func (r *RuntimeInstance) addTxDuration(txSimContext protocol.TxSimContext, fingerprint string,
	duration *utils.TxDuration) {
	// if it is a query tx, fingerprint is "", not record this tx
	if fingerprint == "" {
		return
	}

	if txSimContext.GetDepth() == 0 {
		r.DockerManager.BlockDurationMgr.AddTx(fingerprint, duration)
	} else {
		r.DockerManager.BlockDurationMgr.AddCrossTx(fingerprint, duration)
	}

}
