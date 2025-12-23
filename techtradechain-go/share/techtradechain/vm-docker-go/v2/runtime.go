/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package docker_go

import (
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"techtradechain.com/techtradechain/common/v2/bytehelper"
	commonCrt "techtradechain.com/techtradechain/common/v2/cert"
	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	bcx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	vmPb "techtradechain.com/techtradechain/pb-go/v2/vm"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-docker-go/v2/config"
	"techtradechain.com/techtradechain/vm-docker-go/v2/gas"
	"techtradechain.com/techtradechain/vm-docker-go/v2/pb/protogo"
	"techtradechain.com/techtradechain/vm-docker-go/v2/utils"

	"github.com/gogo/protobuf/proto"
)

const (
	mountContractDir        = "contracts"
	msgIterIsNil            = "iterator is nil"
	txTimeout               = 9000 // tx execution timeout(milliseconds)
	version2201      uint32 = 2201
	version2210      uint32 = 2210
	version2220      uint32 = 2220
	version2310      uint32 = 2030100
)

var (
	chainConfigContractName = syscontract.SystemContract_CHAIN_CONFIG.String()
	keyChainConfig          = chainConfigContractName
)

type ClientManager interface {
	PutTxRequest(txRequest *protogo.CDMMessage)

	PutSysCallResponse(sysCallResp *protogo.CDMMessage)

	RegisterReceiveChan(chainId, txId string, receiveCh chan *protogo.CDMMessage) error

	DeleteReceiveChan(chainId, txId string) bool

	GetVMConfig() *config.DockerVMConfig

	GetUniqueTxKey(txId string) string

	NeedSendContractByteCode() bool

	HasActiveConnections() bool
}

// RuntimeInstance docker-go runtime
type RuntimeInstance struct {
	rowIndex      int32  // iterator index
	ChainId       string // chain id
	ClientManager ClientManager
	Log           protocol.Logger
	DockerManager *DockerManager
}

// Invoke process one tx in docker and return result
// nolint: gocyclo, revive
func (r *RuntimeInstance) Invoke(contract *commonPb.Contract, method string,
	byteCode []byte, parameters map[string][]byte, txSimContext protocol.TxSimContext,
	gasUsed uint64) (contractResult *commonPb.ContractResult, execOrderTxType protocol.ExecOrderTxType) {

	originalTxId := txSimContext.GetTx().Payload.TxId

	uniqueTxKey := r.ClientManager.GetUniqueTxKey(originalTxId)

	// contract response
	contractResult = &commonPb.ContractResult{
		Code:    uint32(1),
		Result:  nil,
		Message: "",
	}

	if !r.ClientManager.HasActiveConnections() {
		r.Log.Errorf("cdm client stream not ready, waiting reconnect, tx id: %s", originalTxId)
		err := errors.New("cdm client not connected")
		return r.errorResult(contractResult, err, err.Error())
	}

	if r.ClientManager.GetVMConfig().DisableInstall && method == protocol.ContractInitMethod {
		r.Log.Errorf("%s docker_go has disabled contract install, pls check techtradechain.yml", originalTxId)
		err := errors.New("contract install disabled in techtradechain.yml")
		return r.errorResult(contractResult, nil, err.Error())
	}

	if r.ClientManager.GetVMConfig().DisableUpgrade && method == protocol.ContractUpgradeMethod {
		r.Log.Errorf("%s docker_go has disabled contract upgrade, pls check techtradechain.yml", originalTxId)
		err := errors.New("contract upgrade disabled in techtradechain.yml")
		return r.errorResult(contractResult, nil, err.Error())
	}

	specialTxType := protocol.ExecOrderTxTypeNormal

	var err error
	// init func gas used calc and check gas limit
	if txSimContext.GetBlockVersion() < version2310 {
		gasUsed, err = gas.InitFuncGasUsedLT2310(gasUsed, parameters)
	} else {
		gasUsed, err = gas.InitFuncGasUsed(gasUsed, parameters)
	}
	if err != nil {
		contractResult.GasUsed = gasUsed
		return r.errorResult(contractResult, err, err.Error())
	}

	//init contract gas used calc and check gas limit
	gasUsed, err = gas.ContractGasUsed(txSimContext, gasUsed, method, contract.Name, byteCode)
	if err != nil {
		contractResult.GasUsed = gasUsed
		return r.errorResult(contractResult, err, err.Error())
	}

	for key := range parameters {
		if strings.Contains(key, "CONTRACT") {
			delete(parameters, key)
		}
	}

	// construct cdm message
	txRequest := &protogo.TxRequest{
		ChainId:         r.ChainId,
		TxId:            uniqueTxKey,
		ContractName:    contract.Name,
		ContractVersion: contract.Version,
		Method:          method,
		Parameters:      parameters,
		TxContext: &protogo.TxContext{
			CurrentHeight:       0,
			OriginalProcessName: "",
			WriteMap:            nil,
			ReadMap:             nil,
		},
	}
	cdmMessage := &protogo.CDMMessage{
		ChainId:   r.ChainId,
		TxId:      uniqueTxKey,
		Type:      protogo.CDMType_CDM_TYPE_TX_REQUEST,
		TxRequest: txRequest,
	}

	// register result chan
	responseCh := make(chan *protogo.CDMMessage, 1)
	err = r.ClientManager.RegisterReceiveChan(r.ChainId, uniqueTxKey, responseCh)
	if err != nil {
		return r.errorResult(contractResult, err, err.Error())
	}

	// init time statistics
	startTime := time.Now()
	txElapsedTime := NewTxElapsedTime(uniqueTxKey, startTime.UnixNano())

	defer func() {
		// add time statistics
		txElapsedTime.TotalTime = time.Since(startTime).Nanoseconds()
		r.Log.Debugf(txElapsedTime.ToString())

		requestId := txSimContext.GetBlockFingerprint()
		// if it is a query tx, requestId is "", not record this tx
		if requestId != "" {
			r.DockerManager.RequestMgr.AddTx(requestId, txElapsedTime)
		}
	}()

	// send message to tx chan
	r.ClientManager.PutTxRequest(cdmMessage)

	timeoutC := time.After(txTimeout * time.Millisecond)

	r.Log.Debugf("start tx [%s] in runtime", txRequest.TxId)
	// wait this chan
	for {
		select {
		case recvMsg := <-responseCh:

			// init syscall time statistics
			sysCallStart := time.Now()
			var storageTime int64
			sysCallElapsedTime := NewSysCallElapsedTime(recvMsg.Type, sysCallStart.UnixNano(), 0, 0)
			txElapsedTime.AddToSysCallList(sysCallElapsedTime)

			// do syscall according to msg type
			switch recvMsg.Type {
			case protogo.CDMType_CDM_TYPE_GET_BYTECODE:
				r.Log.Debugf("tx [%s] start get bytecode [%v]", uniqueTxKey, recvMsg)
				getByteCodeResponse, readStorageTime := r.handleGetByteCodeRequest(uniqueTxKey, recvMsg, byteCode, txSimContext)
				storageTime = readStorageTime
				r.ClientManager.PutSysCallResponse(getByteCodeResponse)
				r.Log.Debugf("tx [%s] finish get bytecode", uniqueTxKey)

			case protogo.CDMType_CDM_TYPE_GET_STATE:
				r.Log.Debugf("tx [%s] start get state [%v]", uniqueTxKey, recvMsg)
				getStateResponse, readStorageTime, pass := r.handleGetStateRequest(uniqueTxKey, recvMsg, txSimContext)
				storageTime = readStorageTime
				if pass {
					gasUsed, err = gas.GetStateGasUsed(gasUsed, getStateResponse.Payload)
					if err != nil {
						getStateResponse.ResultCode = protocol.ContractSdkSignalResultFail
						getStateResponse.Payload = nil
						getStateResponse.Message = err.Error()
					}
				}
				r.ClientManager.PutSysCallResponse(getStateResponse)
				r.Log.Debugf("tx [%s] finish get state [%v]", uniqueTxKey, getStateResponse)

			case protogo.CDMType_CDM_TYPE_GET_BATCH_STATE:
				r.Log.Debugf("tx [%s] start get state [%v]", uniqueTxKey, recvMsg)
				getStateResponse, readStorageTime, pass := r.handleGetBatchStateRequest(uniqueTxKey, recvMsg, txSimContext)
				storageTime = readStorageTime
				if pass {
					gasUsed, err = gas.GetBatchStateGasUsed(gasUsed, getStateResponse.Payload)
					if err != nil {
						getStateResponse.ResultCode = protocol.ContractSdkSignalResultFail
						getStateResponse.Payload = nil
						getStateResponse.Message = err.Error()
					}
				}
				r.ClientManager.PutSysCallResponse(getStateResponse)
				r.Log.Debugf("tx [%s] finish get state [%v]", uniqueTxKey, getStateResponse)

			case protogo.CDMType_CDM_TYPE_TX_RESPONSE:
				r.Log.Debugf("[%s] start handle response [%v]", uniqueTxKey, recvMsg)
				// construct response
				txResponse := recvMsg.TxResponse

				// add time statistics
				defer func() {
					sysCallElapsedTime.TotalTime = time.Since(sysCallStart).Nanoseconds()
					sysCallElapsedTime.StorageTimeInSysCall = storageTime
					txElapsedTime.AddSysCallElapsedTime(sysCallElapsedTime)
					if txResponse.TxElapsedTime != nil {
						txElapsedTime.CrossCallCnt = txResponse.TxElapsedTime.CrossCallCnt
						txElapsedTime.CrossCallTime = txResponse.TxElapsedTime.CrossCallTime
					}
				}()

				// tx fail, just return without merge read write map and events
				if txResponse.Code != 0 {
					contractResult.Code = 1
					contractResult.Result = txResponse.Result
					contractResult.Message = txResponse.Message
					contractResult.GasUsed = gasUsed
					r.Log.Errorf("[%s] return error response [%v]", uniqueTxKey, contractResult)
					return contractResult, protocol.ExecOrderTxTypeNormal
				}

				contractResult.Code = 0
				contractResult.Result = txResponse.Result
				contractResult.Message = txResponse.Message

				// merge read map to sim context
				r.mergeSimContextReadMap(txSimContext, txResponse.GetReadMap())

				// merge write map to sim context
				gasUsed, err = r.mergeSimContextWriteMap(txSimContext, txResponse.GetWriteMap(), gasUsed)
				if err != nil {
					contractResult.GasUsed = gasUsed
					r.Log.Errorf("[%s] return error response [%v]", uniqueTxKey, contractResult)
					return r.errorResult(contractResult, err, "fail to put in sim context")
				}

				// merge events
				var contractEvents []*commonPb.ContractEvent

				if txSimContext.GetBlockVersion() < 2300 && len(txResponse.Events) > protocol.EventDataMaxCount-1 {
					err = fmt.Errorf("too many event data")
					r.Log.Errorf("[%s] return error response [%v]", uniqueTxKey, contractResult)
					return r.errorResult(contractResult, err, "fail to put event data")
				}

				for _, event := range txResponse.Events {
					contractEvent := &commonPb.ContractEvent{
						Topic:           event.Topic,
						TxId:            originalTxId,
						ContractName:    event.ContractName,
						ContractVersion: event.ContractVersion,
						EventData:       event.Data,
					}

					// emit event gas used calc and check gas limit
					gasUsed, err = gas.EmitEventGasUsed(gasUsed, contractEvent)
					if err != nil {
						r.Log.Errorf("[%s] return error response [%v]", uniqueTxKey, contractResult)
						contractResult.GasUsed = gasUsed
						return r.errorResult(contractResult, err, err.Error())
					}

					contractEvents = append(contractEvents, contractEvent)
				}

				contractResult.GasUsed = gasUsed
				contractResult.ContractEvent = contractEvents

				r.Log.Debugf("[%s] finish handle response [%v]", uniqueTxKey, contractResult)

				return contractResult, specialTxType

			case protogo.CDMType_CDM_TYPE_CREATE_KV_ITERATOR:
				r.Log.Debugf("tx [%s] start create kv iterator [%v]", uniqueTxKey, recvMsg)
				var createKvIteratorResponse *protogo.CDMMessage
				specialTxType = protocol.ExecOrderTxTypeIterator
				createKvIteratorResponse, gasUsed = r.handleCreateKvIterator(uniqueTxKey, recvMsg, txSimContext, gasUsed)

				r.ClientManager.PutSysCallResponse(createKvIteratorResponse)
				r.Log.Debugf("tx [%s] finish create kv iterator [%v]", uniqueTxKey, createKvIteratorResponse)

			case protogo.CDMType_CDM_TYPE_CONSUME_KV_ITERATOR:
				r.Log.Debugf("tx [%s] start consume kv iterator [%v]", uniqueTxKey, recvMsg)
				var consumeKvIteratorResponse *protogo.CDMMessage
				consumeKvIteratorResponse, gasUsed = r.handleConsumeKvIterator(uniqueTxKey, recvMsg, txSimContext, gasUsed)

				r.ClientManager.PutSysCallResponse(consumeKvIteratorResponse)
				r.Log.Debugf("tx [%s] finish consume kv iterator [%v]", uniqueTxKey, consumeKvIteratorResponse)

			case protogo.CDMType_CDM_TYPE_CREATE_KEY_HISTORY_ITER:
				r.Log.Debugf("tx [%s] start create key history iterator [%v]", uniqueTxKey, recvMsg)
				var createKeyHistoryIterResp *protogo.CDMMessage
				specialTxType = protocol.ExecOrderTxTypeIterator
				createKeyHistoryIterResp, gasUsed = r.handleCreateKeyHistoryIterator(uniqueTxKey, recvMsg, txSimContext, gasUsed)
				r.ClientManager.PutSysCallResponse(createKeyHistoryIterResp)
				r.Log.Debugf("tx [%s] finish create key history iterator [%v]", uniqueTxKey, createKeyHistoryIterResp)

			case protogo.CDMType_CDM_TYPE_CONSUME_KEY_HISTORY_ITER:
				r.Log.Debugf("tx [%s] start consume key history iterator [%v]", uniqueTxKey, recvMsg)
				var consumeKeyHistoryResp *protogo.CDMMessage
				consumeKeyHistoryResp, gasUsed = r.handleConsumeKeyHistoryIterator(uniqueTxKey, recvMsg, txSimContext, gasUsed)
				r.ClientManager.PutSysCallResponse(consumeKeyHistoryResp)
				r.Log.Debugf("tx [%s] finish consume key history iterator [%v]", uniqueTxKey, consumeKeyHistoryResp)

			case protogo.CDMType_CDM_TYPE_GET_SENDER_ADDRESS:
				r.Log.Debugf("tx [%s] start get sender address [%v]", uniqueTxKey, recvMsg)
				var getSenderAddressResp *protogo.CDMMessage
				getSenderAddressResp, gasUsed = r.handleGetSenderAddress(uniqueTxKey, txSimContext, gasUsed)
				r.ClientManager.PutSysCallResponse(getSenderAddressResp)
				r.Log.Debugf("tx [%s] finish get sender address [%v]", uniqueTxKey, getSenderAddressResp)

			case protogo.CDMType_CDM_TYPE_GET_CONTRACT_NAME:
				r.Log.Debugf("tx [%s] start get contract name [%v]", uniqueTxKey, recvMsg)
				var getContractNameResp *protogo.CDMMessage
				getContractNameResp, readStorageTime := r.handleGetContractName(uniqueTxKey, recvMsg, txSimContext)
				storageTime = readStorageTime

				r.ClientManager.PutSysCallResponse(getContractNameResp)
				r.Log.Debugf("tx [%s] finish get contract name [%v]", uniqueTxKey, getContractNameResp)

			default:
				contractResult.GasUsed = gasUsed
				return r.errorResult(
					contractResult,
					fmt.Errorf("unknow type"),
					"fail to receive request",
				)
			}

			// end time statistics; modify total spend time and storage time
			sysCallElapsedTime.TotalTime = time.Since(sysCallStart).Nanoseconds()
			sysCallElapsedTime.StorageTimeInSysCall = storageTime
			txElapsedTime.AddSysCallElapsedTime(sysCallElapsedTime)
			r.Log.Debug(txElapsedTime.ToString(), txElapsedTime.PrintSysCallList())

		case <-timeoutC:
			deleted := r.ClientManager.DeleteReceiveChan(r.ChainId, uniqueTxKey)
			if deleted {
				r.Log.Errorf("[%s] fail to receive response in 10 seconds and return timeout response",
					uniqueTxKey)
				r.Log.Infof(txElapsedTime.ToString())
				r.Log.InfoDynamic(func() string {
					return txElapsedTime.PrintSysCallList()
				})
				contractResult.GasUsed = gasUsed
				return r.errorResult(contractResult, fmt.Errorf("tx timeout"),
					"fail to receive response",
				)
			}
		}
	}
}

func (r *RuntimeInstance) newEmptyResponse(txId string, msgType protogo.CDMType) *protogo.CDMMessage {
	return &protogo.CDMMessage{
		TxId:       txId,
		Type:       msgType,
		ResultCode: protocol.ContractSdkSignalResultFail,
		Payload:    nil,
		Message:    "",
		ChainId:    r.ChainId,
	}
}

func (r *RuntimeInstance) handleGetSenderAddress(txId string,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.CDMMessage, uint64) {
	getSenderAddressResponse := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_GET_SENDER_ADDRESS_RESPONSE)

	var err error
	gasUsed, err = gas.GetSenderAddressGasUsed(gasUsed)
	if err != nil {
		getSenderAddressResponse.ResultCode = protocol.ContractSdkSignalResultFail
		getSenderAddressResponse.Message = err.Error()
		getSenderAddressResponse.Payload = nil
		return getSenderAddressResponse, gasUsed
	}

	var bytes []byte
	bytes, err = txSimContext.Get(chainConfigContractName, []byte(keyChainConfig))
	if err != nil {
		r.Log.Errorf("txSimContext get failed, name[%s] key[%s] err: %s",
			chainConfigContractName, keyChainConfig, err.Error())
		getSenderAddressResponse.ResultCode = protocol.ContractSdkSignalResultFail
		getSenderAddressResponse.Message = err.Error()
		getSenderAddressResponse.Payload = nil
		return getSenderAddressResponse, gasUsed
	}

	var chainConfig configPb.ChainConfig
	if err = proto.Unmarshal(bytes, &chainConfig); err != nil {
		r.Log.Errorf("unmarshal chainConfig failed, contractName %s err: %+v", chainConfigContractName, err)
		getSenderAddressResponse.ResultCode = protocol.ContractSdkSignalResultFail
		getSenderAddressResponse.Message = err.Error()
		getSenderAddressResponse.Payload = nil
		return getSenderAddressResponse, gasUsed
	}

	/*
		| memberType            | memberInfo |
		| ---                   | ---        |
		| MemberType_CERT       | PEM        |
		| MemberType_CERT_HASH  | HASH       |
		| MemberType_PUBLIC_KEY | PEM        |
		| MemberType_ALIAS      | ALIAS      |
	*/

	var address string
	address, err = r.getSenderAddrWithBlockVersion(txSimContext.GetBlockVersion(), chainConfig, txSimContext)
	if err != nil {
		r.Log.Error(err.Error())
		getSenderAddressResponse.ResultCode = protocol.ContractSdkSignalResultFail
		getSenderAddressResponse.Message = err.Error()
		getSenderAddressResponse.Payload = nil
		return getSenderAddressResponse, gasUsed
	}

	r.Log.Debug("get sender address: ", address)
	getSenderAddressResponse.ResultCode = protocol.ContractSdkSignalResultSuccess
	getSenderAddressResponse.Payload = []byte(address)

	return getSenderAddressResponse, gasUsed
}

func (r *RuntimeInstance) getSenderAddrWithBlockVersion(blockVersion uint32, chainConfig configPb.ChainConfig,
	txSimContext protocol.TxSimContext) (string, error) {
	var address string
	var err error

	sender := txSimContext.GetSender()

	switch sender.MemberType {
	case accesscontrol.MemberType_CERT:
		address, err = r.getSenderAddressFromCert(blockVersion, sender.MemberInfo, chainConfig.Vm.AddrType)
		if err != nil {
			r.Log.Errorf("getSenderAddressFromCert failed, %s", err.Error())
			return "", err
		}
	case accesscontrol.MemberType_CERT_HASH,
		accesscontrol.MemberType_ALIAS:
		if blockVersion < version2201 && sender.MemberType == accesscontrol.MemberType_ALIAS {
			r.Log.Error("handleGetSenderAddress failed, invalid member type")
			return "", err
		}

		address, err = r.getSenderAddressFromCertHash(
			blockVersion,
			sender.MemberInfo,
			chainConfig.Vm.AddrType,
			txSimContext,
		)
		if err != nil {
			r.Log.Errorf("getSenderAddressFromCert failed, %s", err.Error())
			return "", err
		}

	case accesscontrol.MemberType_PUBLIC_KEY:
		address, err = r.getSenderAddressFromPublicKeyPEM(blockVersion, sender.MemberInfo, chainConfig.Vm.AddrType,
			crypto.HashAlgoMap[chainConfig.GetCrypto().Hash])
		if err != nil {
			r.Log.Errorf("getSenderAddressFromPublicKeyPEM failed, %s", err.Error())
			return "", err
		}

	default:
		r.Log.Errorf("getSenderAddrWithBlockVersion failed, invalid member type")
		return "", err
	}

	return address, nil
}

func (r *RuntimeInstance) getSenderAddressFromCertHash(blockVersion uint32, memberInfo []byte,
	addressType configPb.AddrType, txSimContext protocol.TxSimContext) (string, error) {
	var certBytes []byte
	var err error
	certBytes, err = r.getCertFromChain(memberInfo, txSimContext)
	if err != nil {
		return "", err
	}

	var address string
	address, err = r.getSenderAddressFromCert(blockVersion, certBytes, addressType)
	if err != nil {
		r.Log.Errorf("getSenderAddressFromCert failed, %s", err.Error())
		return "", err
	}

	return address, nil
}

func (r *RuntimeInstance) getCertFromChain(memberInfo []byte, txSimContext protocol.TxSimContext) ([]byte, error) {
	certHashKey := hex.EncodeToString(memberInfo)
	certBytes, err := txSimContext.Get(syscontract.SystemContract_CERT_MANAGE.String(), []byte(certHashKey))
	if err != nil {
		r.Log.Errorf("get cert from chain failed, %s", err.Error())
		return nil, err
	}

	return certBytes, nil
}

func (r *RuntimeInstance) getSenderAddressFromCert(blockVersion uint32, certPem []byte,
	addressType configPb.AddrType) (string, error) {
	if addressType == configPb.AddrType_ZXL {
		address, err := evmutils.ZXAddressFromCertificatePEM(certPem)
		if err != nil {
			return "", fmt.Errorf("ParseCertificate failed, %s", err.Error())
		}

		return address, nil
	}

	if blockVersion >= version2220 {
		if addressType == configPb.AddrType_TECHTRADECHAIN {
			return r.calculateCertAddr2220(certPem)
		}

		return "", errors.New("invalid address type")
	}

	if blockVersion == version2201 || blockVersion == version2210 {
		if addressType == configPb.AddrType_TECHTRADECHAIN {
			return r.calculateCertAddrBefore2220(certPem)
		}

		return "", errors.New("invalid address type")
	}

	if blockVersion < version2201 {
		if addressType == configPb.AddrType_ETHEREUM {
			return r.calculateCertAddrBefore2220(certPem)
		}

		return "", errors.New("invalid address type")
	}

	return "", errors.New("invalid address type")
}

func (r *RuntimeInstance) calculateCertAddrBefore2220(certPem []byte) (string, error) {
	blockCrt, _ := pem.Decode(certPem)
	crt, err := bcx509.ParseCertificate(blockCrt.Bytes)
	if err != nil {
		return "", fmt.Errorf("MakeAddressFromHex failed, %s", err.Error())
	}

	ski := hex.EncodeToString(crt.SubjectKeyId)
	addrInt, err := evmutils.MakeAddressFromHex(ski)
	if err != nil {
		return "", fmt.Errorf("MakeAddressFromHex failed, %s", err.Error())
	}

	return addrInt.String(), nil
}

func (r *RuntimeInstance) calculateCertAddr2220(certPem []byte) (string, error) {
	blockCrt, _ := pem.Decode(certPem)
	crt, err := bcx509.ParseCertificate(blockCrt.Bytes)
	if err != nil {
		return "", fmt.Errorf("MakeAddressFromHex failed, %s", err.Error())
	}

	ski := hex.EncodeToString(crt.SubjectKeyId)
	addrInt, err := evmutils.MakeAddressFromHex(ski)
	if err != nil {
		return "", fmt.Errorf("MakeAddressFromHex failed, %s", err.Error())
	}

	addr := evmutils.BigToAddress(addrInt)
	addrBytes := addr[:]

	return hex.EncodeToString(addrBytes), nil
}

func (r *RuntimeInstance) getSenderAddressFromPublicKeyPEM(blockVersion uint32, publicKeyPem []byte,
	addressType configPb.AddrType, hashType crypto.HashType) (string, error) {
	if addressType == configPb.AddrType_ZXL {
		address, err := evmutils.ZXAddressFromPublicKeyPEM(publicKeyPem)
		if err != nil {
			r.Log.Errorf("ZXAddressFromPublicKeyPEM, failed, %s", err.Error())
		}
		return address, err
	}

	if blockVersion >= version2220 {
		if addressType == configPb.AddrType_TECHTRADECHAIN {
			return r.calculatePubKeyAddr2220(publicKeyPem, hashType)
		}

		return "", errors.New("invalid address type")
	}

	if blockVersion == version2201 || blockVersion == version2210 {
		if addressType == configPb.AddrType_TECHTRADECHAIN {
			return r.calculatePubKeyAddrBefore2220(publicKeyPem, hashType)
		}

		return "", errors.New("invalid address type")
	}

	if blockVersion < version2201 {
		if addressType == configPb.AddrType_ETHEREUM {
			return r.calculatePubKeyAddrBefore2220(publicKeyPem, hashType)
		}

		return "", errors.New("invalid address type")
	}

	return "", errors.New("invalid address type")
}

func (r *RuntimeInstance) calculatePubKeyAddrBefore2220(publicKeyPem []byte, hashType crypto.HashType) (string, error) {
	publicKey, err := asym.PublicKeyFromPEM(publicKeyPem)
	if err != nil {
		return "", fmt.Errorf("ParsePublicKey failed, %s", err.Error())
	}

	ski, err := commonCrt.ComputeSKI(hashType, publicKey.ToStandardKey())
	if err != nil {
		return "", fmt.Errorf("computeSKI from public key failed, %s", err.Error())
	}

	addr, err := evmutils.MakeAddressFromHex(hex.EncodeToString(ski))
	if err != nil {
		return "", fmt.Errorf("make address from cert SKI failed, %s", err)
	}
	return addr.String(), nil
}

func (r *RuntimeInstance) calculatePubKeyAddr2220(publicKeyPem []byte, hashType crypto.HashType) (string, error) {
	publicKey, err := asym.PublicKeyFromPEM(publicKeyPem)
	if err != nil {
		return "", fmt.Errorf("ParsePublicKey failed, %s", err.Error())
	}

	ski, err := commonCrt.ComputeSKI(hashType, publicKey.ToStandardKey())
	if err != nil {
		return "", fmt.Errorf("computeSKI from public key failed, %s", err.Error())
	}

	addrInt, err := evmutils.MakeAddressFromHex(hex.EncodeToString(ski))
	if err != nil {
		return "", fmt.Errorf("make address from public key failed, %s", err)
	}

	addr := evmutils.BigToAddress(addrInt)
	addrBytes := addr[:]

	return hex.EncodeToString(addrBytes), nil
}

func (r *RuntimeInstance) handleCreateKeyHistoryIterator(txId string, recvMsg *protogo.CDMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.CDMMessage, uint64) {

	createKeyHistoryIterResponse := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_CREATE_KEY_HISTORY_TER_RESPONSE)

	/*
		| index | desc          |
		| ----  | ----          |
		| 0     | contractName  |
		| 1     | key           |
		| 2     | field         |
		| 3     | writeMapCache |
	*/
	keyList := strings.SplitN(string(recvMsg.Payload), "#", 4)
	calledContractName := keyList[0]
	keyStr := keyList[1]
	field := keyList[2]
	writeMapBytes := keyList[3]

	writeMap := make(map[string][]byte)
	var err error

	gasUsed, err = gas.CreateKeyHistoryIterGasUsed(gasUsed)
	if err != nil {
		createKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.Message = err.Error()
		createKeyHistoryIterResponse.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	if err = json.Unmarshal([]byte(writeMapBytes), &writeMap); err != nil {
		r.Log.Errorf("get write map failed, %s", err.Error())
		createKeyHistoryIterResponse.Message = err.Error()
		createKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	gasUsed, err = r.mergeSimContextWriteMap(txSimContext, writeMap, gasUsed)
	if err != nil {
		r.Log.Errorf("merge the sim context write map failed, %s", err.Error())
		createKeyHistoryIterResponse.Message = err.Error()
		createKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	if err = protocol.CheckKeyFieldStr(keyStr, field); err != nil {
		r.Log.Errorf("invalid key field str, %s", err.Error())
		createKeyHistoryIterResponse.Message = err.Error()
		createKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	key := protocol.GetKeyStr(keyStr, field)

	iter, err := txSimContext.GetHistoryIterForKey(calledContractName, key)
	if err != nil {
		createKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		createKeyHistoryIterResponse.Payload = nil
		return createKeyHistoryIterResponse, gasUsed
	}

	index := atomic.AddInt32(&r.rowIndex, 1)
	txSimContext.SetIterHandle(index, iter)

	r.Log.Debug("create key history iterator: ", index)

	createKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultSuccess
	createKeyHistoryIterResponse.Payload = bytehelper.IntToBytes(index)

	return createKeyHistoryIterResponse, gasUsed
}

func (r *RuntimeInstance) handleConsumeKeyHistoryIterator(txId string, recvMsg *protogo.CDMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.CDMMessage, uint64) {
	consumeKeyHistoryIterResponse := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_CONSUME_KEY_HISTORY_ITER_RESPONSE)

	currentGasUsed, err := gas.ConsumeKvIteratorGasUsed(gasUsed)
	if err != nil {
		consumeKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.Message = err.Error()
		consumeKeyHistoryIterResponse.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}

	/*
		|	index	|			desc				|
		|	----	|			----  				|
		|	 0  	|	consumeKvIteratorFunc		|
		|	 1  	|		rsIndex					|
	*/

	keyList := strings.Split(string(recvMsg.Payload), "#")
	consumeKeyHistoryIteratorFunc := keyList[0]
	keyHistoryIterIndex, err := bytehelper.BytesToInt([]byte(keyList[1]))
	if err != nil {
		r.Log.Errorf("failed to get iterator index, %s", err.Error())
		consumeKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.Message = err.Error()
		consumeKeyHistoryIterResponse.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}

	iter, ok := txSimContext.GetIterHandle(keyHistoryIterIndex)
	if !ok {
		errMsg := fmt.Sprintf("[key history iterator consume] can not found iterator index [%d]", keyHistoryIterIndex)
		r.Log.Error(errMsg)

		consumeKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.Message = errMsg
		consumeKeyHistoryIterResponse.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}

	keyHistoryIterator, ok := iter.(protocol.KeyHistoryIterator)
	if !ok {
		errMsg := "assertion failed"
		r.Log.Error(errMsg)

		consumeKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.Message = errMsg
		consumeKeyHistoryIterResponse.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}

	switch consumeKeyHistoryIteratorFunc {
	case config.FuncKeyHistoryIterHasNext:
		return keyHistoryIterHasNext(keyHistoryIterator, gasUsed, consumeKeyHistoryIterResponse)

	case config.FuncKeyHistoryIterNext:
		return keyHistoryIterNext(keyHistoryIterator, gasUsed, consumeKeyHistoryIterResponse)

	case config.FuncKeyHistoryIterClose:
		return keyHistoryIterClose(keyHistoryIterator, gasUsed, consumeKeyHistoryIterResponse)
	default:
		consumeKeyHistoryIterResponse.ResultCode = protocol.ContractSdkSignalResultFail
		consumeKeyHistoryIterResponse.Message = fmt.Sprintf("%s not found", consumeKeyHistoryIteratorFunc)
		consumeKeyHistoryIterResponse.Payload = nil
		return consumeKeyHistoryIterResponse, currentGasUsed
	}
}

func keyHistoryIterHasNext(iter protocol.KeyHistoryIterator, gasUsed uint64,
	response *protogo.CDMMessage) (*protogo.CDMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKeyHistoryIterGasUsed(gasUsed)
	if err != nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = err.Error()
		response.Payload = nil
		return response, gasUsed
	}

	hasNext := config.BoolFalse
	if iter.Next() {
		hasNext = config.BoolTrue
	}

	response.ResultCode = protocol.ContractSdkSignalResultSuccess
	response.Payload = bytehelper.IntToBytes(int32(hasNext))

	return response, gasUsed
}

func keyHistoryIterNext(iter protocol.KeyHistoryIterator, gasUsed uint64,
	response *protogo.CDMMessage) (*protogo.CDMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKeyHistoryIterGasUsed(gasUsed)
	if err != nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = err.Error()
		response.Payload = nil
		return response, gasUsed
	}

	if iter == nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = msgIterIsNil
		response.Payload = nil
		return response, gasUsed
	}

	var historyValue *store.KeyModification
	historyValue, err = iter.Value()
	if err != nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = err.Error()
		response.Payload = nil
		return response, gasUsed
	}

	response.ResultCode = protocol.ContractSdkSignalResultSuccess
	blockHeight := bytehelper.IntToBytes(int32(historyValue.BlockHeight))
	timestampStr := strconv.FormatInt(historyValue.Timestamp, 10)
	isDelete := config.BoolTrue
	if !historyValue.IsDelete {
		isDelete = config.BoolFalse
	}

	/*
		| index | desc        |
		| ---   | ---         |
		| 0     | txId        |
		| 1     | blockHeight |
		| 2     | isDelete    |
		| 3     | timestamp   |
		| 4     | value       |
	*/
	response.Payload = func() []byte {
		str := historyValue.TxId + "#" +
			string(blockHeight) + "#" +
			string(bytehelper.IntToBytes(int32(isDelete))) + "#" +
			timestampStr + "#" +
			string(historyValue.Value)
		return []byte(str)
	}()

	return response, gasUsed
}

func keyHistoryIterClose(iter protocol.KeyHistoryIterator, gasUsed uint64,
	response *protogo.CDMMessage) (*protogo.CDMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKeyHistoryIterGasUsed(gasUsed)
	if err != nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = err.Error()
		response.Payload = nil
		return response, gasUsed
	}

	iter.Release()
	response.ResultCode = protocol.ContractSdkSignalResultSuccess
	response.Payload = nil

	return response, gasUsed
}

func (r *RuntimeInstance) handleConsumeKvIterator(txId string, recvMsg *protogo.CDMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.CDMMessage, uint64) {

	consumeKvIteratorResponse := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_CONSUME_KV_ITERATOR_RESPONSE)

	/*
		|	index	|			desc				|
		|	----	|			----  				|
		|	 0  	|	consumeKvIteratorFunc		|
		|	 1  	|		rsIndex					|
	*/

	keyList := strings.Split(string(recvMsg.Payload), "#")
	consumeKvIteratorFunc := keyList[0]
	kvIteratorIndex, err := bytehelper.BytesToInt([]byte(keyList[1]))
	if err != nil {
		r.Log.Errorf("failed to get iterator index, %s", err.Error())
		gasUsed, err = gas.ConsumeKvIteratorGasUsed(gasUsed)
		if err != nil {
			consumeKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			consumeKvIteratorResponse.Message = err.Error()
			consumeKvIteratorResponse.Payload = nil
			return consumeKvIteratorResponse, gasUsed
		}
		return consumeKvIteratorResponse, gasUsed
	}

	iter, ok := txSimContext.GetIterHandle(kvIteratorIndex)
	if !ok {
		r.Log.Errorf("[kv iterator consume] can not found iterator index [%d]", kvIteratorIndex)
		consumeKvIteratorResponse.Message = fmt.Sprintf(
			"[kv iterator consume] can not found iterator index [%d]", kvIteratorIndex,
		)
		gasUsed, err = gas.ConsumeKvIteratorGasUsed(gasUsed)
		if err != nil {
			consumeKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			consumeKvIteratorResponse.Message = err.Error()
			consumeKvIteratorResponse.Payload = nil
			return consumeKvIteratorResponse, gasUsed
		}
		return consumeKvIteratorResponse, gasUsed
	}

	kvIterator, ok := iter.(protocol.StateIterator)
	if !ok {
		r.Log.Errorf("assertion failed")
		consumeKvIteratorResponse.Message = fmt.Sprintf(
			"[kv iterator consume] failed, iterator %d assertion failed", kvIteratorIndex,
		)
		gasUsed, err = gas.ConsumeKvIteratorGasUsed(gasUsed)
		if err != nil {
			consumeKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			consumeKvIteratorResponse.Message = err.Error()
			consumeKvIteratorResponse.Payload = nil
			return consumeKvIteratorResponse, gasUsed
		}
		return consumeKvIteratorResponse, gasUsed
	}

	switch consumeKvIteratorFunc {
	case config.FuncKvIteratorHasNext:
		return kvIteratorHasNext(kvIterator, gasUsed, consumeKvIteratorResponse)

	case config.FuncKvIteratorNext:
		return kvIteratorNext(kvIterator, gasUsed, consumeKvIteratorResponse)

	case config.FuncKvIteratorClose:
		return kvIteratorClose(kvIterator, gasUsed, consumeKvIteratorResponse)

	default:
		consumeKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
		consumeKvIteratorResponse.Message = fmt.Sprintf("%s not found", consumeKvIteratorFunc)
		consumeKvIteratorResponse.Payload = nil
		return consumeKvIteratorResponse, gasUsed
	}
}

func kvIteratorHasNext(kvIterator protocol.StateIterator, gasUsed uint64,
	response *protogo.CDMMessage) (*protogo.CDMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKvIteratorGasUsed(gasUsed)
	if err != nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = err.Error()
		response.Payload = nil
		return response, gasUsed
	}

	hasNext := config.BoolFalse
	if kvIterator.Next() {
		hasNext = config.BoolTrue
	}

	response.ResultCode = protocol.ContractSdkSignalResultSuccess
	response.Payload = bytehelper.IntToBytes(int32(hasNext))

	return response, gasUsed
}

func kvIteratorNext(kvIterator protocol.StateIterator, gasUsed uint64,
	response *protogo.CDMMessage) (*protogo.CDMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKvIteratorGasUsed(gasUsed)
	if err != nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = err.Error()
		response.Payload = nil
		return response, gasUsed
	}

	if kvIterator == nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = msgIterIsNil
		response.Payload = nil
		return response, gasUsed
	}

	var kvRow *store.KV
	kvRow, err = kvIterator.Value()
	if err != nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = err.Error()
		response.Payload = nil
		return response, gasUsed
	}

	arrKey := strings.Split(string(kvRow.Key), "#")
	key := arrKey[0]
	field := ""
	if len(arrKey) > 1 {
		field = arrKey[1]
	}

	value := kvRow.Value

	response.ResultCode = protocol.ContractSdkSignalResultSuccess
	response.Payload = func() []byte {
		str := key + "#" + field + "#" + string(value)
		return []byte(str)
	}()

	return response, gasUsed
}

func kvIteratorClose(kvIterator protocol.StateIterator, gasUsed uint64,
	response *protogo.CDMMessage) (*protogo.CDMMessage, uint64) {
	var err error
	gasUsed, err = gas.ConsumeKvIteratorGasUsed(gasUsed)
	if err != nil {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		response.Message = err.Error()
		response.Payload = nil
		return response, gasUsed
	}

	kvIterator.Release()
	response.ResultCode = protocol.ContractSdkSignalResultSuccess
	response.Payload = nil

	return response, gasUsed
}

func (r *RuntimeInstance) mergeSimContextReadMap(txSimContext protocol.TxSimContext,
	readMap map[string][]byte) {

	for key, value := range readMap {
		var contractName string
		var contractKey string
		var contractField string
		keyList := strings.Split(key, "#")
		contractName = keyList[0]
		contractKey = keyList[1]
		if len(keyList) == 3 {
			contractField = keyList[2]
		}

		txSimContext.PutIntoReadSet(contractName, protocol.GetKeyStr(contractKey, contractField), value)
	}
}

func (r *RuntimeInstance) mergeSimContextWriteMap(txSimContext protocol.TxSimContext,
	writeMap map[string][]byte, gasUsed uint64) (uint64, error) {
	// merge the sim context write map

	for key, value := range writeMap {
		var contractName string
		var contractKey string
		var contractField string
		keyList := strings.Split(key, "#")
		contractName = keyList[0]
		contractKey = keyList[1]
		if len(keyList) == 3 {
			contractField = keyList[2]
		}
		// put state gas used calc and check gas limit
		var err error
		gasUsed, err = gas.PutStateGasUsed(gasUsed, contractName, contractKey, contractField, value)
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

func kvIteratorCreate(txSimContext protocol.TxSimContext, calledContractName string,
	key []byte, limitKey, limitField string, gasUsed uint64) (protocol.StateIterator, uint64, error) {
	var err error
	gasUsed, err = gas.CreateKvIteratorGasUsed(gasUsed)
	if err != nil {
		return nil, gasUsed, err
	}

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

func (r *RuntimeInstance) handleCreateKvIterator(txId string, recvMsg *protogo.CDMMessage,
	txSimContext protocol.TxSimContext, gasUsed uint64) (*protogo.CDMMessage, uint64) {

	createKvIteratorResponse := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_CREATE_KV_ITERATOR_RESPONSE)

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
	keyList := strings.SplitN(string(recvMsg.Payload), "#", 7)
	calledContractName := keyList[0]
	createFunc := keyList[1]
	startKey := keyList[2]
	startField := keyList[3]
	writeMapBytes := keyList[6]

	writeMap := make(map[string][]byte)
	var err error
	if err = json.Unmarshal([]byte(writeMapBytes), &writeMap); err != nil {
		r.Log.Errorf("get WriteMap failed, %s", err.Error())
		createKvIteratorResponse.Message = err.Error()
		gasUsed, err = gas.CreateKvIteratorGasUsed(gasUsed)
		if err != nil {
			createKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	}

	gasUsed, err = r.mergeSimContextWriteMap(txSimContext, writeMap, gasUsed)
	if err != nil {
		r.Log.Errorf("merge the sim context write map failed, %s", err.Error())
		createKvIteratorResponse.Message = err.Error()
		gasUsed, err = gas.CreateKvIteratorGasUsed(gasUsed)
		if err != nil {
			createKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	}

	if err = protocol.CheckKeyFieldStr(startKey, startField); err != nil {
		r.Log.Errorf("invalid key field str, %s", err.Error())
		createKvIteratorResponse.Message = err.Error()
		gasUsed, err = gas.CreateKvIteratorGasUsed(gasUsed)
		if err != nil {
			createKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	}

	key := protocol.GetKeyStr(startKey, startField)

	var iter protocol.StateIterator
	switch createFunc {
	case config.FuncKvIteratorCreate:
		limitKey := keyList[4]
		limitField := keyList[5]
		iter, gasUsed, err = kvIteratorCreate(txSimContext, calledContractName, key, limitKey, limitField, gasUsed)
		if err != nil {
			r.Log.Errorf("failed to create kv iterator, %s", err.Error())
			createKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.Message = err.Error()
			createKvIteratorResponse.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	case config.FuncKvPreIteratorCreate:
		gasUsed, err = gas.CreateKvIteratorGasUsed(gasUsed)
		if err != nil {
			createKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.Message = err.Error()
			createKvIteratorResponse.Payload = nil
			return createKvIteratorResponse, gasUsed
		}

		keyStr := string(key)
		limitLast := keyStr[len(keyStr)-1] + 1
		limit := keyStr[:len(keyStr)-1] + string(limitLast)
		iter, err = txSimContext.Select(calledContractName, key, []byte(limit))
		if err != nil {
			r.Log.Errorf("failed to create kv pre iterator, %s", err.Error())
			createKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultFail
			createKvIteratorResponse.Message = err.Error()
			createKvIteratorResponse.Payload = nil
			return createKvIteratorResponse, gasUsed
		}
	}

	index := atomic.AddInt32(&r.rowIndex, 1)
	txSimContext.SetIterHandle(index, iter)

	r.Log.Debug("create kv iterator: ", index)
	createKvIteratorResponse.ResultCode = protocol.ContractSdkSignalResultSuccess
	createKvIteratorResponse.Payload = bytehelper.IntToBytes(index)

	return createKvIteratorResponse, gasUsed
}

func (r *RuntimeInstance) handleGetByteCodeRequest(txId string, recvMsg *protogo.CDMMessage,
	byteCode []byte, txSimContext protocol.TxSimContext) (*protogo.CDMMessage, int64) {

	var err error
	var storageTime int64

	response := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_GET_BYTECODE_RESPONSE)

	contractNameAndVersion := string(recvMsg.Payload)               // e.g: chain1#contract1#1.0.0
	contractName := utils.SplitContractName(contractNameAndVersion) // e.g: contract1

	if len(byteCode) == 0 {
		r.Log.Warnf("[%s] bytecode is missing", txId)
		startTime := time.Now()
		byteCode, err = txSimContext.GetContractBytecode(contractName)
		spend := time.Since(startTime).Nanoseconds()
		storageTime += spend
		if err != nil || len(byteCode) == 0 {
			r.Log.Errorf("[%s] fail to get contract bytecode: %s, required contract name is: [%s]", txId, err,
				contractName)
			if err != nil {
				response.Message = err.Error()
			} else {
				response.Message = "contract byte is nil"
			}

			return response, storageTime
		}
	}

	hostMountPath := r.ClientManager.GetVMConfig().DockerVMMountPath
	hostMountPath = filepath.Join(hostMountPath, r.ChainId)

	contractDir := filepath.Join(hostMountPath, mountContractDir)
	contractZipPath := filepath.Join(contractDir, fmt.Sprintf("%s.7z", contractName)) // contract1.7z
	contractPathWithoutVersion := filepath.Join(contractDir, contractName)
	contractPathWithVersion := filepath.Join(contractDir, contractNameAndVersion)

	_, err = os.Stat(contractPathWithVersion)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			// file may or may not exist, just run into another problem.
			r.Log.Errorf("read file failed", err)
			response.Message = err.Error()
			return response, storageTime
		}

		// save bytecode to disk
		err = r.saveBytesToDisk(byteCode, contractZipPath)
		if err != nil {
			r.Log.Errorf("[%s] fail to save bytecode to path [%s]: %s", txId, contractZipPath, err)
			response.Message = err.Error()
			return response, storageTime
		}

		// extract 7z file
		unzipCommand := fmt.Sprintf("7z e %s -o%s -y", contractZipPath, contractDir) // e.g: contract1
		err = r.runCmd(unzipCommand)
		if err != nil {
			r.Log.Errorf("[%s] fail to extract contract: %s, extract command: [%s]", txId, err, unzipCommand)
			response.Message = err.Error()
			return response, storageTime
		}

		// remove 7z file
		err = os.Remove(contractZipPath)
		if err != nil {
			r.Log.Errorf("[%s] fail to remove zipped file: %s, path of should removed file is: [%s]", txId, err,
				contractZipPath)
			response.Message = err.Error()
			return response, storageTime
		}

		// replace contract name to contractName:version
		err = os.Rename(contractPathWithoutVersion, contractPathWithVersion)
		if err != nil {
			r.Log.Errorf("[%s] fail to rename contract name: %s, "+
				"please make sure contract name should be same as contract name (first input name) while compiling",
				txId, err)
			response.Message = err.Error()
			return response, storageTime
		}

	}

	if r.ClientManager.NeedSendContractByteCode() {
		contractByteCode, err := ioutil.ReadFile(contractPathWithVersion)
		if err != nil {
			r.Log.Errorf("fail to load contract executable file: %s, ", err)
			response.Message = err.Error()
			return response, storageTime
		}

		response.ResultCode = protocol.ContractSdkSignalResultSuccess
		response.Payload = contractByteCode
	} else {
		response.ResultCode = protocol.ContractSdkSignalResultSuccess
		response.Payload = []byte(contractNameAndVersion)
	}

	return response, storageTime
}

func (r *RuntimeInstance) handleGetContractName(txId string, recvMsg *protogo.CDMMessage,
	txSimContext protocol.TxSimContext) (*protogo.CDMMessage, int64) {
	var err error
	var storageTime int64

	response := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_GET_CONTRACT_NAME_RESPONSE)

	startTime := time.Now()
	contractInfo, err := txSimContext.GetContractByName(string(recvMsg.Payload))
	storageTime = time.Since(startTime).Nanoseconds()

	if err != nil || contractInfo.Name == "" {
		response.ResultCode = protocol.ContractSdkSignalResultFail
		if err != nil {
			response.Message = err.Error()
		} else {
			response.Message = fmt.Sprintf("%v contract not exist", recvMsg.Payload)
		}
		response.Payload = nil
		return response, storageTime
	}

	contractNameAndAddress := strings.Join([]string{contractInfo.Name, contractInfo.Address}, "#")

	response.Payload = []byte(contractNameAndAddress)
	response.ResultCode = protocol.ContractSdkSignalResultSuccess

	return response, storageTime
}

func (r *RuntimeInstance) handleGetStateRequest(txId string, recvMsg *protogo.CDMMessage,
	txSimContext protocol.TxSimContext) (*protogo.CDMMessage, int64, bool) {

	response := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_GET_STATE_RESPONSE)

	var contractName string
	var contractKey string
	var contractField string
	var value []byte
	var err error

	keyList := strings.Split(string(recvMsg.Payload), "#")
	contractName = keyList[0]
	contractKey = keyList[1]
	if len(keyList) == 3 {
		contractField = keyList[2]
	}

	startTime := time.Now()
	value, err = txSimContext.Get(contractName, protocol.GetKeyStr(contractKey, contractField))
	spend := time.Since(startTime).Nanoseconds()

	if err != nil {
		r.Log.Errorf("fail to get state from sim context: %s", err)
		response.Message = err.Error()
		return response, spend, false
	}

	r.Log.Debug("get value: ", string(value))
	response.ResultCode = protocol.ContractSdkSignalResultSuccess
	response.Payload = value
	return response, spend, true
}

func (r *RuntimeInstance) handleGetBatchStateRequest(txId string, recvMsg *protogo.CDMMessage,
	txSimContext protocol.TxSimContext) (*protogo.CDMMessage, int64, bool) {
	var err error
	var payload []byte
	var getKeys []*vmPb.BatchKey
	var storageTime int64

	response := r.newEmptyResponse(txId, protogo.CDMType_CDM_TYPE_GET_BATCH_STATE_RESPONSE)

	keys := &vmPb.BatchKeys{}
	if err = keys.Unmarshal(recvMsg.Payload); err != nil {
		response.Message = err.Error()
		return response, storageTime, false
	}

	startTime := time.Now()
	getKeys, err = txSimContext.GetKeys(keys.Keys)
	storageTime = time.Since(startTime).Nanoseconds()
	if err != nil {
		response.Message = err.Error()
		return response, storageTime, false
	}

	r.Log.Debugf("get batch keys values: %v", getKeys)
	resp := vmPb.BatchKeys{Keys: getKeys}
	payload, err = resp.Marshal()
	if err != nil {
		response.Message = err.Error()
		return response, storageTime, false
	}

	response.ResultCode = protocol.ContractSdkSignalResultSuccess
	response.Payload = payload
	return response, storageTime, true
}

func (r *RuntimeInstance) errorResult(contractResult *commonPb.ContractResult,
	err error, errMsg string) (*commonPb.ContractResult, protocol.ExecOrderTxType) {
	contractResult.Code = uint32(1)
	if err != nil {
		errMsg += ", " + err.Error()
	}
	contractResult.Message = errMsg
	r.Log.Error(errMsg)
	return contractResult, protocol.ExecOrderTxTypeNormal
}

func (r *RuntimeInstance) saveBytesToDisk(bytes []byte, newFilePath string) error {

	f, err := os.Create(newFilePath)
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
func (r *RuntimeInstance) runCmd(command string) error {
	commands := strings.Split(command, " ")
	cmd := exec.Command(commands[0], commands[1:]...) // #nosec

	if err := cmd.Start(); err != nil {
		return err
	}

	return cmd.Wait()
}
