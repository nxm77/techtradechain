/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

//Package evm for evm core runtime
package evm

import (
	"encoding/hex"
	"errors"
	"fmt"
	"runtime/debug"

	"techtradechain.com/techtradechain/logger/v2"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"

	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	pbac "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	evmGo "techtradechain.com/techtradechain/vm-evm/v2/evm-go"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/opcodes"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/storage"
)

// RuntimeInstance evm runtime
type RuntimeInstance struct {
	Method  string // invoke contract method
	ChainId string // chain id
	//Address       *evmutils.Int      //address
	Contract      *commonPb.Contract // contract info
	Log           protocol.Logger
	TxSimContext  protocol.TxSimContext
	ContractEvent []*commonPb.ContractEvent
}

// InstancesManager manager of evm instances
type InstancesManager struct {
}

// NewRuntimeInstance new an evm instance
func (*InstancesManager) NewRuntimeInstance(txSimContext protocol.TxSimContext, chainId, method string, codePath string,
	contract *commonPb.Contract, byteCode []byte, log protocol.Logger) (protocol.RuntimeInstance, error) {

	return &RuntimeInstance{
		Method:        method,
		ChainId:       chainId,
		Contract:      contract,
		TxSimContext:  txSimContext,
		Log:           log, //这里是vm log，打印的是系统日志， EvmLog打印的是合约日志
		ContractEvent: []*commonPb.ContractEvent{},
	}, nil
}

//func getZXLAddr(memberType acPb.MemberType, memberInfo []byte) (addr *evmutils.Int, pk crypto.PublicKey, err error) {
//	if memberType == acPb.MemberType_ADDR {
//		addr = evmutils.FromHexString(string(memberInfo))
//		return addr, nil, nil
//	}
//
//	if memberType == acPb.MemberType_CERT_HASH || memberType == acPb.MemberType_CERT {
//		//虽然simTxContext.getSender()获得的 member type是cert hash类型，但虚拟机外部传参时已经被替换为cert
//		createBlock, _ := pem.Decode(memberInfo)
//		createCert, e := bcx509.ParseCertificate(createBlock.Bytes)
//		if e != nil {
//			return nil, nil, errors.New("failed to parse cert")
//		}
//		pk = createCert.PublicKey
//	} else if memberType == acPb.MemberType_PUBLIC_KEY {
//		pk, err = asym.PublicKeyFromPEM(memberInfo)
//		if err != nil {
//			return nil, nil, errors.New("failed to parse public key")
//		}
//	} else {
//		return nil, nil, errors.New("unsupported access control member types")
//	}
//
//	var addrStr string
//	addrStr, err = evmutils.ZXAddressFromPublicKey(pk)
//	if err != nil {
//		return nil, nil, errors.New("calculate zxl address for creator pk fail")
//	}
//
//	addr = evmutils.FromHexString(addrStr[2:])
//	return addr, pk, err
//}

//func GetCrossParams(parameters map[string][]byte) (bool, map[string][]byte) {
//	isCrossCall := string(parameters[syscontract.CrossParams_CALL_TYPE.String()]) == syscontract.CallType_CROSS.String()
//	if !isCrossCall {
//		return false, nil
//	}
//
//	crossParams := make(map[string][]byte, 8)
//	method, ok := parameters[storage.CrossVmCallMethodKey]
//	if !ok {
//		return false, nil
//	}
//
//	crossParams[strconv.Itoa(0)] = method
//	for k, v := range parameters {
//		i, err := strconv.Atoi(k)
//		if err == nil && 0 < i && i < 8 {
//			crossParams[k] = v
//		}
//	}
//
//	tail := strconv.Itoa(len(crossParams))
//	crossParams[tail] = []byte(storage.CrossVmInParamsEndKey)
//
//	crossData := hex.EncodeToString(evmutils.Keccak256([]byte(storage.CrossVmCallDispatchKey)))[0:8]
//	parameters[protocol.ContractEvmParamKey] = []byte(crossData)
//
//	return isCrossCall, crossParams
//}

//AddrFromBaseParams get address from params
//func AddrFromBaseParams(contract *commonPb.Contract, txSimContext protocol.TxSimContext, addrType config.AddrType,
//	parameters map[string][]byte) (creatorAddr, senderAddr *evmutils.Int, err error) {
//
//	sender := txSimContext.GetSender()
//	if addrType == config.AddrType_ZXL {
//		var creatorPk crypto.PublicKey
//		var senderPk crypto.PublicKey
//		creatorAddr, creatorPk, err = getZXLAddr(contract.Creator.MemberType, parameters[protocol.ContractCreatorPkParam])
//		if err != nil {
//			return nil, nil, errors.New("get creator zxl address fail")
//		}
//		parameters[protocol.ContractCreatorPkParam], _ = creatorPk.Bytes()
//
//		if string(parameters[syscontract.CrossParams_CALL_TYPE.String()]) == syscontract.CallType_CROSS.String() {
//			var senderType int
//			senderType, err = strconv.Atoi(string(parameters[protocol.ContractSenderTypeParam]))
//			if err != nil {
//				return nil, nil, errors.New("parse sender member type failed in cross call")
//			}
//
//			senderAddr, senderPk, err = getZXLAddr(acPb.MemberType(senderType),
//				parameters[syscontract.CrossParams_SENDER.String()])
//			if err != nil {
//				return nil, nil, errors.New("get sender zxl address failed in cross call")
//			}
//		} else {
//			senderAddr, senderPk, err = getZXLAddr(sender.MemberType, parameters[protocol.ContractSenderPkParam])
//			if err != nil {
//				return nil, nil, errors.New("get sender zxl address failed in direct call")
//			}
//		}
//
//		parameters[protocol.ContractSenderPkParam], _ = senderPk.Bytes()
//	} else {
//		var err error
//		creatorAddr, err = evmutils.MakeAddressFromHex(string(parameters[protocol.ContractCreatorPkParam]))
//		if err != nil {
//			return nil, nil, errors.New("get creator pk fail")
//		}
//
//		if string(parameters[syscontract.CrossParams_CALL_TYPE.String()]) == syscontract.CallType_CROSS.String() {
//			senderAddr = evmutils.FromHexString(string(parameters[syscontract.CrossParams_SENDER.String()]))
//		} else {
//			senderAddr, err = evmutils.MakeAddressFromHex(string(parameters[protocol.ContractSenderPkParam]))
//		}
//		if err != nil {
//			return creatorAddr, nil, errors.New("get sender pk fail")
//		}
//	}
//
//	//delete(parameters, protocol.ContractAddrTypeParam)
//	return creatorAddr, senderAddr, nil
//}

func relevantAddress(contract *commonPb.Contract, txSimContext protocol.TxSimContext,
	parameters map[string][]byte) (*evmutils.Int, *evmutils.Int, *evmutils.Int, error) {
	ac, err := txSimContext.GetAccessControl()
	if err != nil {
		return nil, nil, nil, err
	}

	creator := &pbac.Member{
		OrgId:      contract.Creator.OrgId,
		MemberType: contract.Creator.MemberType,
		MemberInfo: contract.Creator.MemberInfo,
	}

	protocolCreator, err1 := ac.NewMember(creator)
	if err1 != nil {
		return nil, nil, nil, err1
	}

	cfg := txSimContext.GetLastChainConfig()
	//cfg, err2 := txSimContext.GetBlockchainStore().GetLastChainConfig()
	//if err2 != nil {
	//	return nil, nil, nil, err2
	//}

	creatorAddr, err3 := utils.GetIntAddrFromMember(protocolCreator, cfg.Vm.AddrType)
	if err3 != nil {
		return nil, nil, nil, err3
	}

	origin := txSimContext.GetSender()
	protocolOrigin, err4 := ac.NewMember(origin)
	if err4 != nil {
		return nil, nil, nil, err4
	}

	originAddr, err5 := utils.GetIntAddrFromMember(protocolOrigin, cfg.Vm.AddrType)
	if err5 != nil {
		return nil, nil, nil, err5
	}

	if string(parameters[syscontract.CrossParams_CALL_TYPE.String()]) == syscontract.CallType_CROSS.String() {
		senderAddr := evmutils.FromHexString(string(parameters[syscontract.CrossParams_SENDER.String()]))
		return creatorAddr, originAddr, senderAddr, nil
	}

	return creatorAddr, originAddr, originAddr, nil
}

// init just load instructions once
func init() {
	// init memory and env
	evmGo.Load()
	// execute method
}

// Invoke contract by call vm, implement protocol.RuntimeInstance
// nolint:gocyclo
func (r *RuntimeInstance) Invoke(contract *commonPb.Contract, method string, byteCode []byte,
	parameters map[string][]byte, txSimContext protocol.TxSimContext, gasUsed uint64) (
	contractResult *commonPb.ContractResult, specialTxType protocol.ExecOrderTxType) {
	txId := txSimContext.GetTx().Payload.TxId
	r.Log.Debugf("evm runtime start to run contract, tx id:%s", txId)
	// contract response
	contractResult = &commonPb.ContractResult{
		Code:    uint32(1),
		Result:  nil,
		Message: "",
	}
	specialTxType = protocol.ExecOrderTxTypeNormal

	defer func() {
		if err := recover(); err != nil {
			r.Log.Errorf("failed to invoke evm, tx id:%s, error:%s", txId, err)
			contractResult.Code = 1
			if e, ok := err.(error); ok {
				contractResult.Message = e.Error()
			} else if e, ok := err.(string); ok {
				contractResult.Message = e
			}
			debug.PrintStack()
		}
	}()

	r.Log.Debugf("evm runtime begin to process params, tx id:%s", txId)
	//此行代码不要挪动位置
	//isCrossCall, crossParams := GetCrossParams(parameters)
	calldata := string(parameters[protocol.ContractEvmParamKey])
	if len(calldata) == 0 && txSimContext.GetBlockVersion() > params.V2030500 {
		// For create and update operations, calldata can be empty
		// For other methods, calldata must not be empty, because calldata must contain the method name.
		if method != protocol.ContractInitMethod && method != protocol.ContractUpgradeMethod {
			return r.errorResult(contractResult, nil, "calldata is null")
		}
	}

	isDeploy := false
	if method == protocol.ContractInitMethod || method == protocol.ContractUpgradeMethod {
		isDeploy = true
	} else {
		if evmutils.Has0xPrefix(method) {
			method = method[2:]
		}

		if txSimContext.GetBlockVersion() < params.V2300 && len(method) != 8 {
			return r.errorResult(contractResult, nil, "contract verify failed, method length is not 8")
		}
	}

	if evmutils.Has0xPrefix(calldata) {
		calldata = calldata[2:]
	}

	if len(calldata)%2 == 1 {
		calldata = "0" + calldata
	}

	messageData, err := hex.DecodeString(calldata)
	if err != nil {
		return r.errorResult(contractResult, err, "params is not hex encode string")
	}

	if isDeploy {
		//if isCrossCall {
		//	messageData = byteCode
		//} else {
		messageData = append(byteCode, messageData...)
		byteCode = messageData
		//}
	}

	r.Log.Debugf("evm runtime begin to get creator, sender and contract addresses, tx id:%s", txId)
	// evmTransaction
	//chainCfg, _ := txSimContext.GetBlockchainStore().GetLastChainConfig()
	chainCfg := txSimContext.GetLastChainConfig()
	creatorAddress, originAddress, senderAddress, err := relevantAddress(contract, txSimContext, parameters)
	if err != nil {
		return r.errorResult(contractResult, err, err.Error())
	}

	gasLeft := protocol.GasLimit - gasUsed
	evmTransaction := environment.Transaction{
		TxHash:   []byte(txId),
		Origin:   originAddress,
		GasPrice: evmutils.New(protocol.EvmGasPrice),
		GasLimit: evmutils.New(int64(gasLeft)),
		BaseFee:  evmutils.New(0),
	}

	// contract
	//address, err := evmutils.MakeAddressFromString(contract.Name) // reference vm_factory.go RunContract
	var addrHexStr string
	if txSimContext.GetBlockVersion() < 2220 || len(contract.Address) == 0 {
		addrHexStr = contract.Name
	} else {
		addrHexStr = contract.Address
	}
	//address, err := name2IntAddr(addrHexStr, chainCfg.Vm.AddrType)
	address, err := addressHexToInt(addrHexStr)
	if err != nil {
		return r.errorResult(contractResult, err, "make address fail")
	}

	r.Log.Debugf("evm runtime begin to get code hash, tx id:%s", txId)
	codeHash := evmutils.BytesDataToEVMIntHash(byteCode)
	eContract := environment.Contract{
		Address: address,
		Code:    byteCode,
		Hash:    codeHash,
		Version: contract.Version,
	}
	//r.Address = address
	r.Contract = contract
	// new evm instance
	r.Log.Debugf("evm runtime begin to new evm instance, tx id:%s", txId)
	//lastBlock, _ := txSimContext.GetBlockchainStore().GetLastBlock()
	blockTimestamp := txSimContext.GetBlockTimestamp()
	blockHeight := txSimContext.GetBlockHeight()
	r.Log.Debugf("evm runtime get last block timestamp:%v, height:%d, tx id:%s", blockTimestamp, blockHeight, txId)

	externalStore := &storage.ContractStorage{
		Ctx:       txSimContext,
		OutParams: storage.NewParamsCache(),
		Contract:  contract,
		SystemLog: r.Log,
		//InParams: &storage.CrossVmParams{
		//	ParamsCache:  crossParams,
		//	IsCrossVm:    isCrossCall,
		//	ParamsBegin:  0,
		//	LongStrLen:   0,
		//	LastParamKey: "",
		//},
	}

	evm := evmGo.New(evmGo.EVMParam{
		MaxStackDepth:  protocol.EvmMaxStackDepth,
		ExternalStore:  externalStore,
		UpperStorage:   nil,
		ResultCallback: r.callback, //will be called as evm.resultNotify when evm.ExecuteContract() end
		Context: &environment.Context{
			Block: environment.Block{
				Coinbase:   creatorAddress, //proposer ski
				Timestamp:  evmutils.New(blockTimestamp),
				Number:     evmutils.New(int64(blockHeight)), // height
				Difficulty: evmutils.New(0),
				GasLimit:   evmutils.New(protocol.GasLimit),
			},
			Contract:    eContract,
			Transaction: evmTransaction,
			Message: environment.Message{
				Caller: senderAddress,
				Value:  evmutils.New(0),
				Data:   messageData,
			},
			Parameters: parameters,
			Cfg: environment.Config{
				AddrType: int32(chainCfg.Vm.AddrType),
				ChainId:  txSimContext.GetTx().Payload.ChainId,
			},
			EvmLog: logger.GetLoggerByChain(logger.MODULE_EVM, r.ChainId),
		},
	})
	// init memory and env
	//evmGo.Load()
	// execute method
	r.Log.Debugf("evm runtime start to execute contract, tx id:%s, isDeploy:%v", txId, isDeploy)
	result, err := evm.ExecuteContract(isDeploy)
	pcCount, timeUsed := evm.GetPcCountAndTimeUsed()
	if err != nil {
		r.Log.Errorf("evm runtime execute contract failed, tx id:%s, pc count:%v, time used:%v, error:%v",
			txId, pcCount, timeUsed, err)
		return r.errorResult(contractResult, err, "failed to execute evm contract")
	}
	r.Log.Debugf("evm runtime execute contract finished, tx id:%s, pc count:%v, time used:%v, isDeploy:%v",
		txId, pcCount, timeUsed, isDeploy)
	contractResult.Code = 0
	contractResult.GasUsed = gasLeft - result.GasLeft
	contractResult.Result = result.ResultData
	contractResult.ContractEvent = r.ContractEvent
	return contractResult, protocol.ExecOrderTxTypeNormal
}

//func contractNameDecimalToAddress(cname string) (*evmutils.Int, error) {
//	// hexStr2 == hexStr2
//	// hexStr := hex.EncodeToString(evmutils.Keccak256([]byte("contractName")))[24:]
//	// hexStr2 := hex.EncodeToString(evmutils.Keccak256([]byte("contractName"))[12:])
//	// 为什么使用十进制字符串转换，因为在./evm-go中，使用的是 address.String()作为key，也就是说数据库的名称是十进制字符串。
//	evmAddr := evmutils.FromDecimalString(cname)
//	if evmAddr == nil {
//		return nil, errors.New("contractName[%s] not DecimalString,
//		you can use evmutils.MakeAddressFromString(\"contractName\").String() get a decimal string")
//	}
//	return evmAddr, nil
//}

func addressHexToInt(cname string) (*evmutils.Int, error) {
	evmAddr := evmutils.FromHexString(cname)
	if evmAddr == nil {
		return nil, errors.New("contractName[%s] not HexString, you can use hex.EncodeToString(" +
			"evmutils.MakeAddressFromString(\"contractName\").Bytes()) get a hex string address")
	}
	return evmAddr, nil
}

//func name2IntAddr(cname string, addrType config.AddrType) (addr *evmutils.Int, err error) {
//	var str string
//
//	if addrType == config.AddrType_ZXL {
//		str, err = evmutils.ZXAddress([]byte(cname))
//		addr = evmutils.FromHexString(str[2:])
//	} else {
//		addr, err = evmutils.MakeAddressFromString(cname)
//	}
//
//	return addr, err
//}

func parseRevertMsg(msg []byte) ([]byte, error) {
	if len(msg) == 0 {
		return nil, fmt.Errorf("no message")
	}

	const (
		funcSignLen = 4
		evmWordLen  = 32
		offsetSite  = funcSignLen + evmWordLen
	)

	iOffset := evmutils.New(0)
	iOffset.SetBytes(msg[funcSignLen:offsetSite])
	offset := iOffset.Uint64()

	iLength := evmutils.New(0)
	iLength.SetBytes(msg[offsetSite : offsetSite+offset])
	length := iLength.Uint64()

	data := msg[36+offset : 36+offset+length]
	return data, nil
}

func (r *RuntimeInstance) callback(result *evmGo.ExecuteResult, err error) {
	v := r.TxSimContext.GetBlockVersion()
	if result.ExitOpCode == opcodes.REVERT {
		if v < params.V2216 {
			err = fmt.Errorf("revert instruction was encountered during execution")
		} else if (params.V2216 <= v && v < params.V2218) || v == params.V2300 || v == params.V2030100 {
			//2300 and 2310 have been released, but 2217 has found bugs, so versions before 2218, as well as 2300 and 2310
			//that have been released, use the old logic, and other versions use the new logic
			msg, _ := parseRevertMsg(result.ResultData)
			err = fmt.Errorf("%s", string(msg))
			result.ResultData = msg
		} else {
			err = fmt.Errorf("%x", string(result.ResultData))
			//fmt.Printf("--------------ErrMsg: %s\n", hex.EncodeToString(result.ResultData))
		}

		r.Log.Errorf("revert instruction encountered in contract [%s] execution, tx: [%s], error: [%s]",
			r.Contract.Name, r.TxSimContext.GetTx().Payload.TxId, err.Error())

		if v < params.V2030400 {
			panic(err)
		}
	}

	if err != nil {
		r.Log.Errorf("error encountered in contract [%s] execution, tx: [%s], error: [%s]",
			r.Contract.Name, r.TxSimContext.GetTx().Payload.TxId, err.Error())

		if v < params.V2030400 {
			panic(err)
		}
	}

	//emit event of contract that be called in cross call
	r.ContractEvent = append(r.ContractEvent, result.StorageCache.ContractEvent...)

	//emit  contract event
	err = r.emitContractEvent(result)
	if err != nil {
		r.Log.Errorf("emit contract event err:%s", err.Error())
		panic(err)
	}

	for n, v := range result.StorageCache.WriteCache {
		for k, val := range v {
			err := r.TxSimContext.Put(n, []byte(k), val.Bytes())
			if err != nil {
				r.Log.Errorf("callback txSimContext put err:%s", err.Error())
			}
			//fmt.Println("n k val", n, k, val, val.String())
		}
	}

	for n, v := range result.StorageCache.ReadCache {
		for k, val := range v {
			r.TxSimContext.PutIntoReadSet(n, []byte(k), val.Bytes())
		}
	}

	r.Log.Debug("result:", result.ResultData)
}

func (r *RuntimeInstance) errorResult(contractResult *commonPb.ContractResult, err error, errMsg string) (
	*commonPb.ContractResult, protocol.ExecOrderTxType) {
	contractResult.Code = 1
	if err != nil {
		errMsg += ", " + err.Error()
	}
	contractResult.Message = errMsg
	r.Log.Error(errMsg)
	return contractResult, protocol.ExecOrderTxTypeNormal
}
func (r *RuntimeInstance) emitContractEvent(result *evmGo.ExecuteResult) error {
	//parse log
	var contractEvents []*commonPb.ContractEvent
	logsMap := result.StorageCache.Logs
	for _, logs := range logsMap {
		for _, log := range logs {
			if len(log.Topics) > protocol.EventDataMaxCount-1 {
				return fmt.Errorf("too many event data")
			}
			contractEvent := &commonPb.ContractEvent{
				TxId:            r.TxSimContext.GetTx().Payload.TxId,
				ContractName:    r.Contract.Name,
				ContractVersion: r.Contract.Version,
			}
			topics := log.Topics
			for index, topic := range topics {
				//the first topic in log as contract event topic,others as event data.
				//in TechTradeChain contract event,only has one topic filed.
				if index == 0 && topic != nil {
					topicHexStr := hex.EncodeToString(topic)
					if err := protocol.CheckTopicStr(topicHexStr); err != nil {
						return fmt.Errorf(err.Error())
					}
					contractEvent.Topic = topicHexStr
					r.Log.Debugf("topicHexString: %s", topicHexStr)
					continue
				}
				//topic marked by 'index' in eth as contract event data
				topicIndexHexStr := hex.EncodeToString(topic)
				r.Log.Debugf("topicIndexString: %s", topicIndexHexStr)
				contractEvent.EventData = append(contractEvent.EventData, topicIndexHexStr)
			}
			data := log.Data
			dataHexStr := hex.EncodeToString(data)
			if len(dataHexStr) > protocol.EventDataMaxLen {
				return fmt.Errorf("event data too long,longer than %v", protocol.EventDataMaxLen)
			}
			contractEvent.EventData = append(contractEvent.EventData, dataHexStr)
			contractEvents = append(contractEvents, contractEvent)
			r.Log.Debugf("dataHexStr: %s", dataHexStr)
		}
	}
	//r.ContractEvent = contractEvents
	r.ContractEvent = append(r.ContractEvent, contractEvents...)
	return nil
}

// StartVM start vm
func (*InstancesManager) StartVM() error {
	return nil
}

// StopVM stop vm
func (*InstancesManager) StopVM() error {
	return nil
}

// BeforeSchedule do sth. before schedule a block
func (*InstancesManager) BeforeSchedule(blockFingerprint string, blockHeight uint64) {
}

// AfterSchedule do sth. after schedule a block
func (*InstancesManager) AfterSchedule(blockFingerprint string, blockHeight uint64) {
}
