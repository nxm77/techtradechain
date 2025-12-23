/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package multisign2320

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"techtradechain.com/techtradechain/pb-go/v2/config"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	"github.com/gogo/protobuf/proto"
)

const (
	blockVersion2312          = uint32(2030102)
	paramNameTruncateValueLen = "truncateValueLen"
	paramNameTruncateModel    = "truncateModel"
)

var (
	contractName = syscontract.SystemContract_MULTI_SIGN.String()
)

// MultiSignContract a multi sign contract
type MultiSignContract struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewMultiSignContract get a multi sign contract
// @param log
// @return *MultiSignContract
func NewMultiSignContract(log protocol.Logger) *MultiSignContract {
	return &MultiSignContract{
		log:     log,
		methods: InitMultiContractMethods(log),
	}
}

// GetMethod get register method by name
func (c *MultiSignContract) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

// InitMultiContractMethods export method
func InitMultiContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	runtime := &MultiSignRuntime{log: log}
	methodMap[syscontract.MultiSignFunction_REQ.String()] = common.WrapResultFunc(runtime.Req)
	methodMap[syscontract.MultiSignFunction_VOTE.String()] = common.WrapEventResult(runtime.Vote)
	methodMap[syscontract.MultiSignFunction_QUERY.String()] = common.WrapResultFunc(runtime.Query)
	methodMap[syscontract.MultiSignFunction_TRIG.String()] = runtime.Trig
	return methodMap
}

// MultiSignRuntime multi sign method
type MultiSignRuntime struct {
	log protocol.Logger
}

// MultiSignRuntimeParam params within multi sign method
type MultiSignRuntimeParam struct {
	truncateValueLen int
	truncateModel    string //hash,truncate,empty
}

func newDefaultMultiSignRuntimeParam() *MultiSignRuntimeParam {
	return &MultiSignRuntimeParam{
		truncateModel:    "",
		truncateValueLen: 0,
	}
}

func (r *MultiSignRuntimeParam) isDefault() bool {
	return r.truncateValueLen == 0
}

// Req request to multi sign, call a native contract
func (r *MultiSignRuntime) Req(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	result []byte, err error) {

	// 获取当前的链配置
	lastChainConfig, err := txSimContext.GetBlockchainStore().GetLastChainConfig()
	if err != nil {
		err2 := fmt.Errorf("get chain config failed, err = %v", err)
		r.log.Warn(err2)
		return nil, err2
	}
	// 检查`3段式`标志位是否开启
	manualRun := getMultiSignEnableManualRun(lastChainConfig)

	if manualRun {
		// 开启`3段式`标志位
		return r.reqWithManualRun(txSimContext, parameters)
	}

	// 未开启了`3段式`标志位
	return r.reqWithoutManualRun(txSimContext, parameters)
}

// Vote voting on existing multiSign transaction requests
// when the enable_manual_run flag is set, call VoteWithManualRun
// when the enable_manual_run flag is not set, call VoteWithoutManualRun
func (r *MultiSignRuntime) Vote(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	result []byte, event []*commonPb.ContractEvent, err error) {

	// 获取当前的链配置
	lastChainConfig, err := txSimContext.GetBlockchainStore().GetLastChainConfig()
	if err != nil {
		err2 := fmt.Errorf("get chain config failed, err = %v", err)
		r.log.Warn(err2)
		return nil, nil, err2
	}
	// 检查`3段式`标志位是否开启
	manualRun := getMultiSignEnableManualRun(lastChainConfig)

	if manualRun {
		// 开启`3段式`标志位
		return r.VoteWithManualRun(txSimContext, parameters)
	}

	// 未开启了`3段式`标志位
	return r.VoteWithoutManualRun(txSimContext, parameters)
}

// getMultiSignInfo get MultiSignInfo object from the blockchain store
func (r *MultiSignRuntime) getMultiSignInfo(
	txSimContext protocol.TxSimContext, multiSignTxId []byte) (*syscontract.MultiSignInfo, error) {
	multiSignInfoBytes, err := txSimContext.Get(contractName, multiSignTxId)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}
	if multiSignInfoBytes == nil {
		return nil, fmt.Errorf("not found tx id[%s]", multiSignTxId)
	}

	multiSignInfo := &syscontract.MultiSignInfo{}
	err = proto.Unmarshal(multiSignInfoBytes, multiSignInfo)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	return multiSignInfo, nil
}

// Trig make the contract call execute, when enable_manual_run flag is `false`
// 1）check the enable_manual_run flag
// 2) check the status of the multi sign tx
// 3) get the txId of the multi sign tx
// 4) compare the sender of Trig with the sender of multi sign tx
func (r *MultiSignRuntime) Trig(
	txSimContext protocol.TxSimContext,
	parameters map[string][]byte) *commonPb.ContractResult {
	// get chainconf
	chainConfig, err := txSimContext.GetBlockchainStore().GetLastChainConfig()
	if err != nil {
		errMsg := fmt.Sprintf("MultiSign::Execute() failed. err = %v", err)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// check the `enable_manual_run` flag
	manualRun := getMultiSignEnableManualRun(chainConfig)
	if !manualRun {
		errMsg := "MultiSign::Execute() failed. reason: enable_manual_run == false"
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// get params
	txId := parameters[syscontract.MultiVote_TX_ID.String()]
	r.log.Infof("multi sign trig start. TX_ID[%s]", txId)

	// unmarshal `multiSignInfo`
	multiSignInfo, err := r.getMultiSignInfo(txSimContext, txId)
	if err != nil {
		errMsg := fmt.Sprintf("not found multiSignInfo, txId = %s, err = %v", txId, err)
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	if multiSignInfo.Status != syscontract.MultiSignStatus_PASSED {
		errMsg := "the status of multiSignInfo is not `PASSED`"
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	// compare req sender & trig sender
	reqTxId := multiSignInfo.Payload.TxId
	reqTx, err := r.getTransactionById(txSimContext, reqTxId)
	if err != nil {
		errMsg := fmt.Sprintf("get multisign_req tx failed, err = %v", err)
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}
	reqSender := reqTx.Sender
	trigSender := txSimContext.GetTx().Sender

	if !bytes.Equal(reqSender.Signer.MemberInfo, trigSender.Signer.MemberInfo) {
		errMsg := "trig sender must be same with req sender"
		r.log.Warn(errMsg)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       0,
			ContractEvent: nil,
		}
	}

	r.log.Infof("multi sign trig verify success")
	// call contract and set status
	contractResult := r.invokeContract(txSimContext, multiSignInfo)
	err = r.saveMultiSignInfo(txSimContext, txId, multiSignInfo)
	if err != nil {
		errMsg := fmt.Sprintf("save multiSignInfo failed, err = %v", err)
		return &commonPb.ContractResult{
			Code:          1,
			Result:        nil,
			Message:       errMsg,
			GasUsed:       contractResult.GasUsed,
			ContractEvent: nil,
		}
	}

	// make user can know the inner contract call is success or failed.
	if txSimContext.GetBlockVersion() >= blockVersion2312 {
		if contractResult.Code > 0 {
			contractResult.Message = "DelegationFailed:" + contractResult.Message
		} else {
			contractResult.Message = "DelegationSuccess:" + contractResult.Message
		}
	}
	// make multiSignInfo can be saved successfully.
	contractResult.Code = 0
	return contractResult
}

// verifySignature check if the endorsers have fulfilled the resourceName's policy
func (r *MultiSignRuntime) verifySignature(ac protocol.AccessControlProvider, resourceName string,
	endorsers []*commonPb.EndorsementEntry, mPayloadByte []byte, blockVersion uint32) (bool, error) {
	principal, err := ac.CreatePrincipal(resourceName, endorsers, mPayloadByte)
	if err != nil {
		r.log.Warn(err)
		return false, err
	}
	endorsement, err := ac.GetValidEndorsements(principal, blockVersion)
	if err != nil {
		r.log.Warn(err)
		return false, err
	}
	if len(endorsement) == 0 {
		err = fmt.Errorf("multi sign vote error,endorsement:%s is invalid", endorsement)
		r.log.Warn(err)
		return false, err
	}
	multiSignVerify, err := ac.VerifyPrincipalLT2330(principal, blockVersion)
	if err != nil {
		r.log.Warn("multi sign vote verify fail.", err)
	}
	return multiSignVerify, nil
}

// hasVoted check if the sender of reqVoteInfo has voted before
func (r *MultiSignRuntime) hasVoted(ac protocol.AccessControlProvider,
	reqVoteInfo *syscontract.MultiSignVoteInfo, multiSignInfo *syscontract.MultiSignInfo, txId []byte) error {
	if multiSignInfo.Status != syscontract.MultiSignStatus_PROCESSING {
		err := fmt.Errorf("the multi sign[%s] has been completed", txId)
		r.log.Warn(err)
		return err
	}

	signer, err := ac.NewMember(reqVoteInfo.Endorsement.Signer)
	if err != nil {
		r.log.Warn(err)
		return err
	}
	signerUid := signer.GetUid()
	for _, info := range multiSignInfo.VoteInfos {
		signed, _ := ac.NewMember(info.Endorsement.Signer)
		if signerUid == signed.GetUid() {
			err = fmt.Errorf("the signer[org:%s] is voted", signed.GetUid())
			r.log.Warn(err)
			return err
		}
	}
	return nil
}

// saveMultiSignInfo save the MultiSignInfo into blockchain store by txSimContext
func (r *MultiSignRuntime) saveMultiSignInfo(
	txSimContext protocol.TxSimContext,
	txId []byte,
	multiSignInfo *syscontract.MultiSignInfo) error {
	// record status
	multiSignInfoBytes, err := multiSignInfo.Marshal()
	if err != nil {
		r.log.Error(err)
		return err
	}
	err = txSimContext.Put(contractName, txId, multiSignInfoBytes)
	if err != nil {
		r.log.Error(err)
		return err
	}

	return nil
}

// getTransactionById get the Transaction by txId
func (r *MultiSignRuntime) getTransactionById(txSimContext protocol.TxSimContext, txId string) (
	*commonPb.Transaction, error) {
	store := txSimContext.GetBlockchainStore()
	txInfo, err := store.GetTxWithInfo(txId)
	if err != nil {
		return nil, err
	}
	if txInfo.Transaction == nil {
		return nil, fmt.Errorf("txInfo.Transaction is nil, txId = %v", txId)
	}
	return txInfo.Transaction, nil
}

// invokeContract 使用跨合约调用的方式进行多签合约执行。
// 合约执行成功：返回合约执行后的事件：
// 合约执行失败：返回 nil
func (r *MultiSignRuntime) invokeContract(txSimContext protocol.TxSimContext,
	multiSignInfo *syscontract.MultiSignInfo) *commonPb.ContractResult {
	txId := txSimContext.GetTx().Payload.TxId
	contract := &commonPb.Contract{
		Name:        multiSignInfo.ContractName,
		RuntimeType: commonPb.RuntimeType_NATIVE, // multi sign only support native contract
		Status:      commonPb.ContractStatus_NORMAL,
		Creator:     nil,
	}

	// 准备参数
	initParam := make(map[string][]byte)
	for _, parameter := range multiSignInfo.Payload.Parameters {
		// is sysContractName or sysMethod continue
		if parameter.Key == syscontract.MultiReq_SYS_CONTRACT_NAME.String() ||
			parameter.Key == syscontract.MultiReq_SYS_METHOD.String() {
			continue
		}
		initParam[parameter.Key] = parameter.Value
	}
	byteCode := initParam[syscontract.InitContract_CONTRACT_BYTECODE.String()]

	// 跨合约调用
	caller, err := txSimContext.GetContractByName(syscontract.SystemContract_MULTI_SIGN.String())
	if err != nil {
		return nil
	}

	contractResult, _, statusCode := txSimContext.CallContract(caller, contract, multiSignInfo.Method, byteCode,
		initParam, 0, commonPb.TxType_INVOKE_CONTRACT)
	if statusCode == commonPb.TxStatusCode_SUCCESS {
		multiSignInfo.Message = "OK"
		multiSignInfo.Status = syscontract.MultiSignStatus_ADOPTED
		multiSignInfo.Result = contractResult.Result
		r.log.Infof("multi sign trig[%s] finished, result: %v", txId, contractResult)
	} else {
		contractErr := errors.New(contractResult.Message)
		multiSignInfo.Message = contractErr.Error()
		multiSignInfo.Status = syscontract.MultiSignStatus_FAILED
		r.log.Warnf("multi sign vote[%s] failed, msg: %s", txId, contractErr)
	}
	return contractResult
}

func (r *MultiSignRuntime) validateParams(parameters map[string][]byte) (*MultiSignRuntimeParam, error) {

	var err error

	//解析传入的各个参数到对象BlockRuntimeParam中
	param := newDefaultMultiSignRuntimeParam()
	for key, v := range parameters {
		switch key {
		case paramNameTruncateValueLen:
			if len(v) == 0 {
				param.truncateValueLen = 0
			} else {
				value := string(v)
				param.truncateValueLen, err = strconv.Atoi(value)
			}
		case paramNameTruncateModel:
			param.truncateModel = string(v)
		}
		if err != nil {
			return nil, err
		}
	}

	return param, nil
}

// Query get multi sign status
func (r *MultiSignRuntime) Query(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	result []byte, err error) {

	r.log.Debugf("multi sign query params = %v", parameters)
	var params *MultiSignRuntimeParam
	if params, err = r.validateParams(parameters); err != nil {
		return nil, err
	}

	txId := parameters[syscontract.MultiVote_TX_ID.String()]
	if utils.IsAnyBlank(txId) {
		err = fmt.Errorf("multi sign query params verify fail. txId cannot be empty")
		return nil, err
	}

	multiSignInfoDB, err := txSimContext.Get(contractName, txId)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	if len(multiSignInfoDB) == 0 {
		return nil, nil
	}

	r.log.Debugf("truncate params = %v", params)
	if !params.isDefault() {
		multiSignInfo := syscontract.MultiSignInfo{}
		if err = proto.Unmarshal(multiSignInfoDB, &multiSignInfo); err != nil {
			return nil, err
		}
		r.log.Debugf("multi sign info = %v", multiSignInfo)

		truncate := common.NewTruncateConfig(params.truncateValueLen, params.truncateModel)
		truncate.TruncatePayload(multiSignInfo.Payload)

		return proto.Marshal(&multiSignInfo)
	}

	return multiSignInfoDB, nil
}

// 支持多签的合约名
// 1）合约管理
// 2）链管理
// 3）证书管理
// 4）账户管理
func supportMultiSign(contractName, method string) bool {
	return contractName == syscontract.SystemContract_CONTRACT_MANAGE.String() ||
		contractName == syscontract.SystemContract_CHAIN_CONFIG.String() ||
		contractName == syscontract.SystemContract_CERT_MANAGE.String() ||
		contractName == syscontract.SystemContract_ACCOUNT_MANAGER.String()
}

func (r *MultiSignRuntime) supportRule(ctx protocol.TxSimContext, name []byte, method []byte) error {
	ac, err := ctx.GetAccessControl()
	if err != nil {
		return err
	}
	resourceName := string(name) + "-" + string(method)
	return ac.IsRuleSupportedByMultiSign(resourceName, ctx.GetBlockVersion())
}

func getMultiSignEnableManualRun(chainConfig *config.ChainConfig) bool {
	if chainConfig.Vm == nil {
		return false
	} else if chainConfig.Vm.Native == nil {
		return false
	} else if chainConfig.Vm.Native.Multisign == nil {
		return false
	}

	return chainConfig.Vm.Native.Multisign.EnableManualRun
}
