/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package contractmgr

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
)

var (
	// ContractName comment at next version
	ContractName = syscontract.SystemContract_CONTRACT_MANAGE.String()
	// TRUE comment at next version
	TRUE = []byte("true")
	// FALSE comment at next version
	FALSE = []byte("false")
)

// ContractManager comment at next version
type ContractManager struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewContractManager comment at next version
// @param log
// @return *ContractManager
func NewContractManager(log protocol.Logger) *ContractManager {
	return &ContractManager{
		log:     log,
		methods: registerContractManagerMethods(log),
	}
}

// GetMethod comment at next version
func (c *ContractManager) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

func registerContractManagerMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	runtime := &ContractManagerRuntime{log: log}
	methodMap[syscontract.ContractManageFunction_INIT_CONTRACT.String()] = runtime.installContract
	methodMap[syscontract.ContractManageFunction_UPGRADE_CONTRACT.String()] = runtime.upgradeContract
	methodMap[syscontract.ContractManageFunction_FREEZE_CONTRACT.String()] = common.WrapResultFunc(
		runtime.freezeContract)
	methodMap[syscontract.ContractManageFunction_UNFREEZE_CONTRACT.String()] = common.WrapResultFunc(
		runtime.unfreezeContract)
	methodMap[syscontract.ContractManageFunction_REVOKE_CONTRACT.String()] = common.WrapResultFunc(
		runtime.revokeContract)
	methodMap[syscontract.ContractQueryFunction_GET_CONTRACT_INFO.String()] = common.WrapResultFunc(
		runtime.getContractInfo)
	methodMap[syscontract.ContractQueryFunction_GET_CONTRACT_LIST.String()] = common.WrapResultFunc(
		runtime.getAllContracts)

	methodMap[syscontract.ContractManageFunction_GRANT_CONTRACT_ACCESS.String()] = common.WrapResultFunc(
		runtime.grantContractAccess)
	methodMap[syscontract.ContractManageFunction_REVOKE_CONTRACT_ACCESS.String()] = common.WrapResultFunc(
		runtime.revokeContractAccess)
	methodMap[syscontract.ContractManageFunction_VERIFY_CONTRACT_ACCESS.String()] = common.WrapResultFunc(
		runtime.verifyContractAccess)
	methodMap[syscontract.ContractQueryFunction_GET_DISABLED_CONTRACT_LIST.String()] = common.WrapResultFunc(
		runtime.getDisabledContractList)
	methodMap[syscontract.ContractManageFunction_INIT_NEW_NATIVE_CONTRACT.String()] = common.WrapResultFunc(
		runtime.InitNewNativeContract)

	return methodMap

}

func generateAddress(context protocol.TxSimContext, name string) string {
	var addr string
	chainCfg, _ := context.GetBlockchainStore().GetLastChainConfig()
	if chainCfg.Vm.AddrType == config.AddrType_ZXL {
		addr, _ = evmutils.ZXAddress([]byte(name))
		addr = addr[2:]
	} else {
		bytesAddr := evmutils.Keccak256([]byte(name))
		addr = hex.EncodeToString(bytesAddr)[24:]
	}

	return addr
}

func putContract(context protocol.TxSimContext, contract *commonPb.Contract) error {
	cdata, err := contract.Marshal()
	if err != nil {
		return err
	}

	//put by name key
	nameKey := utils.GetContractDbKey(contract.Name)
	err = context.Put(ContractName, nameKey, cdata)
	if err != nil {
		return err
	}

	//put by address key
	if context.GetBlockVersion() >= 2220 {
		//without version protection, sync block will generate extra rwset，
		addrKey := utils.GetContractDbKey(contract.Address)
		err = context.Put(ContractName, addrKey, cdata)
		if err != nil {
			return err
		}
	}
	return nil
}

// grantContractAccess enable access to a native contract
// this method will take off the contract name from the disabled contract list
// 传入合约列表，将合约状态改为Normal
func (r *ContractManagerRuntime) grantContractAccess(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var (
		err                 error
		requestContractList []string
	)

	// 1. get the requested contracts to enable access from parameters
	requestContractListBytes := params[syscontract.ContractAccess_NATIVE_CONTRACT_NAME.String()]
	err = json.Unmarshal(requestContractListBytes, &requestContractList)
	if err != nil {
		return nil, err
	}
	for _, cn := range requestContractList {
		if cn == syscontract.SystemContract_CONTRACT_MANAGE.String() {
			return nil, errors.New("can't grant contract_manage")
		}
	}
	// 2. unfreeze contract status
	for _, contractName := range requestContractList {
		_, err := r.UnfreezeContract(txSimContext, contractName)
		if err != nil {
			return nil, err
		}
	}
	r.log.Infof("grant access to contract: %v succeed!", requestContractList)
	return nil, nil
}

// disable access to a native contract
// this method will add the contract names to the disabled contract list
// 传入合约列表，将合约状态改为冻结
func (r *ContractManagerRuntime) revokeContractAccess(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	var (
		err                 error
		requestContractList []string
	)

	// 1. get the requested contracts to disable access from parameters
	requestContractListBytes := params[syscontract.ContractAccess_NATIVE_CONTRACT_NAME.String()]
	err = json.Unmarshal(requestContractListBytes, &requestContractList)
	if err != nil {
		return nil, err
	}
	for _, cn := range requestContractList {
		if cn == syscontract.SystemContract_CONTRACT_MANAGE.String() {
			return nil, errors.New("can't revoke contract_manage")
		}
	}

	// 2. freeze contract
	for _, contractName := range requestContractList {
		_, err := r.FreezeContract(txSimContext, contractName)
		if err != nil {
			return nil, err
		}
	}
	r.log.Infof("revoke access to contract: %v succeed!", requestContractList)
	return nil, nil
}

// verify if access to the requested contract is enabled according to the disabled contract list
// returns true as []byte if so or false otherwise
func (r *ContractManagerRuntime) verifyContractAccess(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	var (
		err                  error
		disabledContractList []string
		contractName         string
	)
	requestContractListBytes, ok := params[syscontract.ContractAccess_NATIVE_CONTRACT_NAME.String()]
	if ok {
		contractName = string(requestContractListBytes)
	} else {
		contractName = txSimContext.GetTx().Payload.ContractName
		if contractName == syscontract.SystemContract_MULTI_SIGN.String() {
			contractName, err = getContractNameForMultiSign(txSimContext.GetTx().Payload.Parameters)
			if err != nil {
				return nil, err
			}
		}
	}
	// 1. fetch the disabled native contract list
	disabledContractList, err = r.fetchDisabledContractList(txSimContext)
	if err != nil {
		return nil, err
	}
	for _, c := range disabledContractList {
		if c == contractName {
			return FALSE, errors.New("the contract is in the disabled contract list")
		}
	}
	return TRUE, nil
}

func getContractNameForMultiSign(params []*commonPb.KeyValuePair) (string, error) {
	for i, pair := range params {
		if pair.Key == syscontract.MultiReq_SYS_CONTRACT_NAME.String() {
			return string(params[i].Value), nil
		}
	}
	return "", errors.New("can't find the contract name for multi sign")
}

// fetch the disabled contract list
// 获得冻结的合约列表
func (r *ContractManagerRuntime) getDisabledContractList(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	var (
		err                       error
		disabledContractList      []string
		disabledContractListBytes []byte
	)

	disabledContractList, err = r.fetchDisabledContractList(txSimContext)
	fmt.Printf("the result is %v\n", disabledContractList)

	if err != nil {
		return nil, err
	}

	disabledContractListBytes, err = json.Marshal(disabledContractList)

	if err != nil {
		return nil, err
	}
	return disabledContractListBytes, nil
}

// helper method to fetch the disabled contract list from genesis config file
// if not initialized or from the database otherwise
func (r *ContractManagerRuntime) fetchDisabledContractList(txSimContext protocol.TxSimContext) ([]string, error) {
	// try to get disabled contract list from database
	contracts, err := r.GetAllContracts(txSimContext)
	if err != nil {
		return nil, err
	}
	var disabledContractList []string
	for _, c := range contracts {
		if c.Status == commonPb.ContractStatus_FROZEN {
			disabledContractList = append(disabledContractList, c.Name)
		}
	}
	return disabledContractList, err
}

func (r *ContractManagerRuntime) getContractInfo(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	name := string(parameters[syscontract.GetContractInfo_CONTRACT_NAME.String()])
	contract, err := r.GetContractInfo(txSimContext, name)
	if err != nil {
		return nil, err
	}
	return json.Marshal(contract)
}

func (r *ContractManagerRuntime) getAllContracts(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	contracts, err := r.GetAllContracts(txSimContext)
	if err != nil {
		return nil, err
	}
	r.log.Debugf("getAllContracts result: %+v", contracts)
	return json.Marshal(contracts)
}
func (r *ContractManagerRuntime) installContract(txSimContext protocol.TxSimContext,
	parameters map[string][]byte) *commonPb.ContractResult {
	name, version, byteCode, runtimeType, err := r.parseParam(parameters)
	if err != nil {
		return common.ResultError(err)
	}
	contract, gas, err := r.InstallContract(txSimContext, name, version, byteCode, runtimeType, parameters)
	if err != nil {
		return common.ResultError(err)
	}
	r.log.Infof("install contract success[name:%s version:%s runtimeType:%d byteCodeLen:%d]", contract.Name,
		contract.Version, contract.RuntimeType, len(byteCode))
	contractBytes, _ := contract.Marshal()
	return common.ResultSuccess(contractBytes, gas)
}

func (r *ContractManagerRuntime) upgradeContract(txSimContext protocol.TxSimContext,
	parameters map[string][]byte) *commonPb.ContractResult {
	name, version, byteCode, runtimeType, err := r.parseParam(parameters)
	if err != nil {
		return common.ResultError(err)
	}
	contract, gas, err := r.UpgradeContract(txSimContext, name, version, byteCode, runtimeType, parameters)
	if err != nil {
		return common.ResultError(err)
	}
	r.log.Infof("upgrade contract success[name:%s version:%s runtimeType:%d byteCodeLen:%d]", contract.Name,
		contract.Version, contract.RuntimeType, len(byteCode))
	contractBytes, _ := contract.Marshal()
	return common.ResultSuccess(contractBytes, gas)
}

func (r *ContractManagerRuntime) parseParam(parameters map[string][]byte) (string, string, []byte,
	commonPb.RuntimeType, error) {
	name := string(parameters[syscontract.InitContract_CONTRACT_NAME.String()])
	version := string(parameters[syscontract.InitContract_CONTRACT_VERSION.String()])
	byteCode := parameters[syscontract.InitContract_CONTRACT_BYTECODE.String()]
	runtime := parameters[syscontract.InitContract_CONTRACT_RUNTIME_TYPE.String()]
	if utils.IsAnyBlank(name, version, byteCode, runtime) {
		return "", "", nil, 0, errors.New("params contractName/version/byteCode/runtimeType cannot be empty")
	}
	runtimeInt := commonPb.RuntimeType_value[string(runtime)]
	if runtimeInt == 0 || int(runtimeInt) >= len(commonPb.RuntimeType_value) {
		return "", "", nil, 0, errors.New("params runtimeType[" + string(runtime) + "] is error")
	}
	runtimeType := commonPb.RuntimeType(runtimeInt)
	return name, version, byteCode, runtimeType, nil
}

func (r *ContractManagerRuntime) freezeContract(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	name := string(parameters[syscontract.GetContractInfo_CONTRACT_NAME.String()])
	contract, err := r.FreezeContract(txSimContext, name)
	if err != nil {
		return nil, err
	}
	r.log.Infof("freeze contract success[name:%s version:%s runtimeType:%d]", contract.Name, contract.Version,
		contract.RuntimeType)
	return json.Marshal(contract)
}
func (r *ContractManagerRuntime) unfreezeContract(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	name := string(parameters[syscontract.GetContractInfo_CONTRACT_NAME.String()])
	contract, err := r.UnfreezeContract(txSimContext, name)
	if err != nil {
		return nil, err
	}
	r.log.Infof("unfreeze contract success[name:%s version:%s runtimeType:%d]", contract.Name, contract.Version,
		contract.RuntimeType)
	return json.Marshal(contract)
}
func (r *ContractManagerRuntime) revokeContract(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	name := string(parameters[syscontract.GetContractInfo_CONTRACT_NAME.String()])
	contract, err := r.RevokeContract(txSimContext, name)
	if err != nil {
		return nil, err
	}
	r.log.Infof("revoke contract success[name:%s version:%s runtimeType:%d]", contract.Name, contract.Version,
		contract.RuntimeType)
	return json.Marshal(contract)
}

// ContractManagerRuntime comment at next version
type ContractManagerRuntime struct {
	log protocol.Logger
}

// GetContractInfo 根据合约名字查询合约的详细信息
func (r *ContractManagerRuntime) GetContractInfo(context protocol.TxSimContext, name string) (*commonPb.Contract,
	error) {
	if utils.IsAnyBlank(name) {
		err := fmt.Errorf("%s, param[contract_name] of get contract not found", common.ErrParams.Error())
		r.log.Warnf(err.Error())
		return nil, err
	}
	return getContractByName(context, name)
}

// GetContractByteCode comment at next version
func (r *ContractManagerRuntime) GetContractByteCode(context protocol.TxSimContext, name string) ([]byte, error) {
	if utils.IsAnyBlank(name) {
		err := fmt.Errorf("%s, param[contract_name] of get contract not found", common.ErrParams.Error())
		r.log.Warnf(err.Error())
		return nil, err
	}
	return context.GetContractBytecode(name)
}

// GetAllContracts 查询所有合约的详细信息
func (r *ContractManagerRuntime) GetAllContracts(context protocol.TxSimContext) ([]*commonPb.Contract, error) {
	keyPre := []byte(utils.PrefixContractInfo)
	limitKey := []byte(utils.PrefixContractInfo)
	limitKey[len(limitKey)-1] = limitKey[len(limitKey)-1] + 1
	it, err := context.Select(syscontract.SystemContract_CONTRACT_MANAGE.String(), keyPre, limitKey)
	if err != nil {
		return nil, err
	}
	defer it.Release()
	var result []*commonPb.Contract
	for it.Next() {
		contract := &commonPb.Contract{}
		kv, err := it.Value()
		if err != nil {
			return nil, err
		}
		err = contract.Unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, contract)
	}
	return result, nil
}

// 存储新合约
func (r *ContractManagerRuntime) saveContract(context protocol.TxSimContext, name, version string, byteCode []byte,
	runTime commonPb.RuntimeType) (*commonPb.Contract, []byte, error) {
	nameKey := utils.GetContractDbKey(name)
	existContract, _ := context.Get(ContractName, nameKey)
	if len(existContract) > 0 { //exist
		return nil, nil, errContractExist
	}

	creator, err := r.getCreator(context)
	if err != nil {
		return nil, nil, err
	}

	contract := &commonPb.Contract{
		Name:        name,
		Version:     version,
		RuntimeType: runTime,
		Status:      commonPb.ContractStatus_NORMAL,
		Creator:     creator,
	}

	if context.GetBlockVersion() >= 2220 {
		contract.Address = generateAddress(context, name)
	}

	//cdata, _ := contract.Marshal()

	//err = context.Put(ContractName, nameKey, cdata)
	//if err != nil {
	//	return nil, nil, err
	//}

	//if context.GetBlockVersion() >= 2220 {
	//	addrKey := utils.GetContractDbKey(addr)
	//	err = context.Put(ContractName, addrKey, cdata)
	//	if err != nil {
	//		return nil, nil, err
	//	}
	//}
	err = putContract(context, contract)
	if err != nil {
		return nil, nil, err
	}

	byteCodeKey := utils.GetContractByteCodeDbKey(name)
	err = context.Put(ContractName, byteCodeKey, byteCode)
	if err != nil {
		return nil, nil, err
	}

	return contract, byteCodeKey, nil
}

// InstallContract 安装新合约
func (r *ContractManagerRuntime) InstallContract(context protocol.TxSimContext, name, version string, byteCode []byte,
	runTime commonPb.RuntimeType, initParameters map[string][]byte) (*commonPb.Contract, uint64, error) {
	if !utils.CheckContractNameFormat(name) {
		return nil, 0, errInvalidContractName
	}

	if context.GetBlockVersion() < 2220 {
		if runTime == commonPb.RuntimeType_EVM && !utils.CheckEvmAddressFormat(name) {
			return nil, 0, errInvalidEvmContractName
		}
	}

	contract, byteCodeKey, err := r.saveContract(context, name, version, byteCode, runTime)
	if err != nil {
		return nil, 0, err
	}

	//实例化合约，并init合约，产生读写集
	result, _, statusCode := context.CallContract(nil, contract, protocol.ContractInitMethod, byteCode, initParameters,
		0, commonPb.TxType_INVOKE_CONTRACT)
	if statusCode != commonPb.TxStatusCode_SUCCESS {
		return nil, 0, fmt.Errorf("%s, %s", errContractInitFail, result.Message)
	}
	if result.Code > 0 { //throw error
		return nil, 0, fmt.Errorf("%s, %s", errContractInitFail, result.Message)
	}
	if runTime == commonPb.RuntimeType_EVM {
		//save bytecode body
		//EVM的特殊处理，在调用构造函数后会返回真正需要存的字节码，这里将之前的字节码覆盖
		if len(result.Result) > 0 {
			err := context.Put(ContractName, byteCodeKey, result.Result)
			if err != nil {
				return nil, 0, fmt.Errorf("%s, %s", errContractInitFail, err)
			}
		}
	}
	return contract, result.GasUsed, nil
}

func (r *ContractManagerRuntime) getCreator(context protocol.TxSimContext) (*accesscontrol.MemberFull, error) {
	sender := context.GetSender()
	if context.GetBlockVersion() == 220 { //兼容历史数据的问题
		return &accesscontrol.MemberFull{
			OrgId:      sender.GetOrgId(),
			MemberType: sender.GetMemberType(),
			MemberInfo: sender.GetMemberInfo(),
		}, nil
	}
	ac, err := context.GetAccessControl()
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}

	member, err := ac.NewMember(sender)
	if err != nil {
		r.log.Warn(err)
		return nil, err
	}
	creator := &accesscontrol.MemberFull{
		OrgId:      sender.GetOrgId(),
		MemberType: sender.GetMemberType(),
		MemberInfo: sender.GetMemberInfo(),
		MemberId:   member.GetMemberId(),
		Role:       string(member.GetRole()),
		Uid:        member.GetUid(),
	}
	return creator, nil
}

// UpgradeContract 升级现有合约
func (r *ContractManagerRuntime) UpgradeContract(context protocol.TxSimContext, name, version string, byteCode []byte,
	runTime commonPb.RuntimeType, upgradeParameters map[string][]byte) (*commonPb.Contract, uint64, error) {
	contract, err := getContractByName(context, name)
	if err != nil {
		return nil, 0, err
	}
	if contract.Version == version {
		return nil, 0, errContractVersionExist
	}
	contract.RuntimeType = runTime
	contract.Version = version
	//update ContractInfo
	//cdata, _ := contract.Marshal()
	//key := utils.GetContractDbKey(name)
	//err = context.Put(ContractName, key, cdata)
	if context.GetBlockVersion() >= 2220 {
		if len(contract.Address) == 0 {
			contract.Address = generateAddress(context, name)
		}
	}

	upgradeParameters["__upgrade_contract_old_contract_name"] = []byte(contract.Name)
	upgradeParameters["__upgrade_contract_old_contract_version"] = []byte(contract.Version)
	upgradeParameters["__upgrade_contract_old_contract_runtime_type"] = []byte(contract.RuntimeType.String())

	err = putContract(context, contract)
	if err != nil {
		return nil, 0, err
	}
	//update Contract Bytecode
	byteCodeKey := utils.GetContractByteCodeDbKey(contract.Name)
	err = context.Put(ContractName, byteCodeKey, byteCode)
	if err != nil {
		return nil, 0, err
	}
	//运行新合约的upgrade方法，产生读写集
	result, _, statusCode := context.CallContract(nil, contract, protocol.ContractUpgradeMethod, byteCode, upgradeParameters,
		0, commonPb.TxType_INVOKE_CONTRACT)
	if statusCode != commonPb.TxStatusCode_SUCCESS {
		return nil, 0, fmt.Errorf("%s, %s", errContractUpgradeFail, result.Message)

	}
	if result.Code > 0 { //throw error
		return nil, 0, fmt.Errorf("%s, %s", errContractUpgradeFail, result.Message)
	}
	if runTime == commonPb.RuntimeType_EVM {
		//save bytecode body
		//EVM的特殊处理，在调用构造函数后会返回真正需要存的字节码，这里将之前的字节码覆盖
		if len(result.Result) > 0 {
			err := context.Put(ContractName, byteCodeKey, result.Result)
			if err != nil {
				return nil, 0, fmt.Errorf("%s, %s", errContractUpgradeFail, err)
			}
		}
	}
	return contract, result.GasUsed, nil
}

// FreezeContract comment at next version
func (r *ContractManagerRuntime) FreezeContract(context protocol.TxSimContext, name string) (
	*commonPb.Contract, error) {
	if name == syscontract.SystemContract_CONTRACT_MANAGE.String() { //合约管理合约不能被冻结
		r.log.Warnf("cannot freeze special contract:%s", name)
		return nil, errInvalidContractName
	}
	return r.changeContractStatus(context, name, commonPb.ContractStatus_NORMAL, commonPb.ContractStatus_FROZEN)
}

// UnfreezeContract comment at next version
func (r *ContractManagerRuntime) UnfreezeContract(context protocol.TxSimContext, name string) (
	*commonPb.Contract, error) {
	return r.changeContractStatus(context, name, commonPb.ContractStatus_FROZEN, commonPb.ContractStatus_NORMAL)
}

// RevokeContract comment at next version
func (r *ContractManagerRuntime) RevokeContract(context protocol.TxSimContext, name string) (
	*commonPb.Contract, error) {
	if utils.IsAnyBlank(name) {
		err := fmt.Errorf("%s, param[contract_name] not found", common.ErrParams.Error())
		r.log.Warnf(err.Error())
		return nil, err
	}
	if name == syscontract.SystemContract_CONTRACT_MANAGE.String() || //某些合约是不能被禁用的
		name == syscontract.SystemContract_CHAIN_CONFIG.String() {
		r.log.Warnf("cannot revoke special contract:%s", name)
		return nil, errInvalidContractName
	}
	contract, err := getContractByName(context, name)
	if err != nil {
		return nil, err
	}
	if contract.Status != commonPb.ContractStatus_NORMAL && contract.Status != commonPb.ContractStatus_FROZEN {
		r.log.Warnf("contract[%s] expect status:NORMAL or FROZEN, actual status:%s",
			name, contract.Status.String())
		return nil, errContractStatusInvalid
	}
	contract.Status = commonPb.ContractStatus_REVOKED
	//cdata, _ := contract.Marshal()
	//key := utils.GetContractDbKey(name)
	//err = context.Put(ContractName, key, cdata)
	err = putContract(context, contract)
	if err != nil {
		return nil, err
	}
	return contract, nil
}

// getContractByName 通过名字从保留读写集的方式获得Contract对象
func getContractByName(context protocol.TxSimContext, name string) (*commonPb.Contract, error) {
	key := utils.GetContractDbKey(name)
	//check name exist
	existContract, _ := context.Get(ContractName, key)
	if len(existContract) == 0 { //not exist
		return nil, errContractNotExist
	}
	contract := &commonPb.Contract{}
	err := contract.Unmarshal(existContract)
	if err != nil {
		return nil, err
	}
	return contract, nil
}

func (r *ContractManagerRuntime) changeContractStatus(context protocol.TxSimContext, name string,
	oldStatus, newStatus commonPb.ContractStatus) (*commonPb.Contract, error) {
	if utils.IsAnyBlank(name) {
		err := fmt.Errorf("%s, param[contract_name] not found", common.ErrParams.Error())
		r.log.Warnf(err.Error())
		return nil, err
	}
	contract, err := getContractByName(context, name)
	if err != nil {
		return nil, err
	}
	if contract.Status != oldStatus {
		msg := fmt.Sprintf("contract[%s] expect status:%s,actual status:%s",
			name, oldStatus.String(), contract.Status.String())
		r.log.Warnf(msg)
		return nil, fmt.Errorf("%s, %s", errContractStatusInvalid, msg)
	}
	contract.Status = newStatus
	//cdata, _ := contract.Marshal()
	//key := utils.GetContractDbKey(name)
	//err = context.Put(ContractName, key, cdata)
	err = putContract(context, contract)
	if err != nil {
		return nil, err
	}
	return contract, nil
}

// InitNewNativeContract comment at next version
func (r *ContractManagerRuntime) InitNewNativeContract(txSimContext protocol.TxSimContext, _ map[string][]byte) (
	[]byte, error) {
	contracts, err := r.GetAllContracts(txSimContext)
	if err != nil {
		return nil, err
	}

	existNativeContracts := make(map[string]bool)
	for _, contract := range contracts {
		if contract.RuntimeType == commonPb.RuntimeType_NATIVE {
			existNativeContracts[contract.Name] = true
		}
	}

	var returnContracts []*commonPb.Contract
	for cname := range syscontract.SystemContract_value {
		_, exist := existNativeContracts[cname]
		if txSimContext.GetBlockVersion() < 2230 {
			if !exist {
				contract := &commonPb.Contract{
					Name:        cname,
					Version:     "v1",
					RuntimeType: commonPb.RuntimeType_NATIVE,
					Status:      commonPb.ContractStatus_NORMAL,
					Creator:     nil,
				}

				key := utils.GetContractDbKey(cname)
				value, _ := contract.Marshal()
				err = txSimContext.Put(ContractName, key, value)
				if err != nil {
					return nil, err
				}

				r.log.Infof("init native contract success[name:%s version:%s runtimeType:%d]",
					contract.Name, contract.Version, contract.RuntimeType)
				returnContracts = append(returnContracts, contract)
			}
		} else {
			var contract *commonPb.Contract
			addr := generateAddress(txSimContext, cname)
			if !exist {
				contract = &commonPb.Contract{
					Name:        cname,
					Version:     "v1",
					RuntimeType: commonPb.RuntimeType_NATIVE,
					Status:      commonPb.ContractStatus_NORMAL,
					Creator:     nil,
				}
			} else {
				contract, err = getContractByName(txSimContext, cname)
				if err != nil {
					return nil, err
				}
			}

			contract.Address = addr
			err = putContract(txSimContext, contract)
			if err != nil {
				return nil, err
			}
			r.log.Infof("init native contract success[name:%s version:%s runtimeType:%d]",
				contract.Name, contract.Version, contract.RuntimeType)
			returnContracts = append(returnContracts, contract)
		}
	}
	return json.Marshal(returnContracts)
}
