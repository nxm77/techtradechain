/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package contractmgr210

import (
	"encoding/json"
	"errors"
	"fmt"

	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
	common210 "techtradechain.com/techtradechain/vm-native/v2/v210/common"
)

var (
	// ContractName 当前合约的合约名
	ContractName                   = syscontract.SystemContract_CONTRACT_MANAGE.String()
	keyContractName                = "_Native_Contract_List"
	contractsForMultiSignWhiteList = []string{
		syscontract.SystemContract_CONTRACT_MANAGE.String(),
	}
	// TRUE bytes true
	TRUE = []byte("true")
	// FALSE bytes false
	FALSE = []byte("false")
)

// ContractManager 提供合约安装、升级、冻结、查询等
type ContractManager struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewContractManager create a new ContractManager instance
// @param log
// @return *ContractManager
func NewContractManager(log protocol.Logger) *ContractManager {
	return &ContractManager{
		log:     log,
		methods: registerContractManagerMethods(log),
	}
}

// GetMethod get register method by name
func (c *ContractManager) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

func registerContractManagerMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	runtime := &ContractManagerRuntime{log: log}
	methodMap[syscontract.ContractManageFunction_INIT_CONTRACT.String()] = common210.WrapResultFunc(
		runtime.installContract)
	methodMap[syscontract.ContractManageFunction_UPGRADE_CONTRACT.String()] = common210.WrapResultFunc(
		runtime.upgradeContract)
	methodMap[syscontract.ContractManageFunction_FREEZE_CONTRACT.String()] = common210.WrapResultFunc(
		runtime.freezeContract)
	methodMap[syscontract.ContractManageFunction_UNFREEZE_CONTRACT.String()] = common210.WrapResultFunc(
		runtime.unfreezeContract)
	methodMap[syscontract.ContractManageFunction_REVOKE_CONTRACT.String()] = common210.WrapResultFunc(
		runtime.revokeContract)
	methodMap[syscontract.ContractQueryFunction_GET_CONTRACT_INFO.String()] = common210.WrapResultFunc(
		runtime.getContractInfo)
	methodMap[syscontract.ContractQueryFunction_GET_CONTRACT_LIST.String()] = common210.WrapResultFunc(
		runtime.getAllContracts)

	methodMap[syscontract.ContractManageFunction_GRANT_CONTRACT_ACCESS.String()] = common210.WrapResultFunc(
		runtime.grantContractAccess)
	methodMap[syscontract.ContractManageFunction_REVOKE_CONTRACT_ACCESS.String()] = common210.WrapResultFunc(
		runtime.revokeContractAccess)
	methodMap[syscontract.ContractManageFunction_VERIFY_CONTRACT_ACCESS.String()] = common210.WrapResultFunc(
		runtime.verifyContractAccess)
	methodMap[syscontract.ContractQueryFunction_GET_DISABLED_CONTRACT_LIST.String()] = common210.WrapResultFunc(
		runtime.getDisabledContractList)

	return methodMap

}

// enable access to a native contract
// this method will take off the contract name from the disabled contract list
func (r *ContractManagerRuntime) grantContractAccess(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {

	var (
		err                      error
		requestContractListBytes []byte
		updatedContractListBytes []byte
		disabledContractList     []string
		requestContractList      []string
		updatedContractList      []string
	)

	// 1. fetch the disabled contract list
	disabledContractList, err = r.fetchDisabledContractList(txSimContext)
	if err != nil {
		return nil, err
	}

	// 2. get the requested contracts to enable access from parameters
	requestContractListBytes = params[syscontract.ContractAccess_NATIVE_CONTRACT_NAME.String()]
	err = json.Unmarshal(requestContractListBytes, &requestContractList)
	if err != nil {
		return nil, err
	}
	for _, cn := range requestContractList {
		if cn == syscontract.SystemContract_CONTRACT_MANAGE.String() {
			return nil, errors.New("can't grant contract_manage")
		}
	}

	// 3. adjust the disabled native contract list per the requested contract names
	updatedContractList = filterContracts(disabledContractList, requestContractList)
	updatedContractListBytes, err = json.Marshal(updatedContractList)
	if err != nil {
		return nil, err
	}

	// 4. store the adjusted native contract list back to the database
	err = storeDisabledContractList(txSimContext, updatedContractListBytes)
	if err != nil {
		return nil, err
	}

	r.log.Infof("grant access to contract: %v succeed!", requestContractList)
	return nil, nil
}

// disable access to a native contract
// this method will add the contract names to the disabled contract list
func (r *ContractManagerRuntime) revokeContractAccess(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	var (
		err                      error
		requestContractListBytes []byte
		updatedContractListBytes []byte
		disabledContractList     []string
		requestContractList      []string
		updatedContractList      []string
	)

	// 1. fetch the disabled contract list
	disabledContractList, err = r.fetchDisabledContractList(txSimContext)
	if err != nil {
		return nil, err
	}

	// 2. get the requested contracts to disable access from parameters
	requestContractListBytes = params[syscontract.ContractAccess_NATIVE_CONTRACT_NAME.String()]
	err = json.Unmarshal(requestContractListBytes, &requestContractList)
	if err != nil {
		return nil, err
	}
	for _, cn := range requestContractList {
		if cn == syscontract.SystemContract_CONTRACT_MANAGE.String() {
			return nil, errors.New("can't revoke contract_manage")
		}
	}

	// 3. adjust the disabled native contract list per the requested contract names
	updatedContractList = append(disabledContractList, requestContractList...)

	updatedContractListBytes, err = json.Marshal(updatedContractList)
	if err != nil {
		return nil, err
	}

	// 4. store the updated native contract list back to the database
	err = storeDisabledContractList(txSimContext, updatedContractListBytes)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// verify if access to the requested contract is enabled according to the disabled contract list
// returns true as []byte if so or false otherwise
func (r *ContractManagerRuntime) verifyContractAccess(txSimContext protocol.TxSimContext,
	params map[string][]byte) ([]byte, error) {
	var (
		err                   error
		disabledContractList  []string
		contractName          string
		method                string
		multiSignContractName string
	)

	// 1. fetch the disabled native contract list
	disabledContractList, err = r.fetchDisabledContractList(txSimContext)
	if err != nil {
		return nil, err
	}

	// 2. get the requested contract name and verify if it's on the disabled native contract list
	contractName = txSimContext.GetTx().Payload.ContractName
	method = txSimContext.GetTx().Payload.Method
	if contractName == syscontract.SystemContract_CONTRACT_MANAGE.String() {
		return TRUE, nil
	}
	for _, cn := range disabledContractList {
		if cn == contractName {
			return FALSE, errors.New("the contract is in the disabled contract list")
		}
	}

	// 3. if the requested contract name is multisignature, get the underlying contract name from
	// the tx payload and verify if it has access
	// if the method name is not req, return true since it does not contain contract names in parameters
	if contractName == syscontract.SystemContract_MULTI_SIGN.String() &&
		method == syscontract.MultiSignFunction_VOTE.String() {
		return TRUE, nil
	}
	if contractName == syscontract.SystemContract_MULTI_SIGN.String() &&
		method == syscontract.MultiSignFunction_QUERY.String() {
		return TRUE, nil
	}
	if contractName == syscontract.SystemContract_MULTI_SIGN.String() &&
		method == syscontract.MultiSignFunction_REQ.String() {

		multiSignContractName, err = getContractNameForMultiSign(txSimContext.GetTx().Payload.Parameters)
		if err != nil {
			return nil, err
		}
		if multiSignContractName == syscontract.SystemContract_CONTRACT_MANAGE.String() {
			return TRUE, nil
		}
		// check if the requested contract is on the disabled contract list
		for _, cn := range disabledContractList {
			if cn == multiSignContractName {
				return FALSE, errors.New("the contract is in the disabled contract list")
			}
		}

		// check if the requested contract list is on the multi sign white list
		for _, cn := range contractsForMultiSignWhiteList {
			if cn == multiSignContractName {
				return TRUE, nil
			}
		}

		return FALSE, errors.New("the contract is not open for multi-sign")
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

// store the disabled contract list to the database
func storeDisabledContractList(txSimContext protocol.TxSimContext, disabledContractListBytes []byte) error {
	var (
		err                              error
		refinedDisabledContractListBytes []byte
		disabledContractList             []string
		refinedDisabledContractList      []string
	)
	err = json.Unmarshal(disabledContractListBytes, &disabledContractList)
	if err != nil {
		return err
	}

	// filter out redundant contract names in the disabled contract list
	uniqueMap := make(map[string]string)
	for _, cn := range disabledContractList {
		if _, ok := uniqueMap[cn]; !ok {
			uniqueMap[cn] = cn
			refinedDisabledContractList = append(refinedDisabledContractList, cn)
		}
	}

	refinedDisabledContractListBytes, err = json.Marshal(refinedDisabledContractList)
	if err != nil {
		return err
	}

	err = txSimContext.Put(ContractName, []byte(keyContractName), refinedDisabledContractListBytes)
	if err != nil {
		return err
	}
	return nil
}

// filter out request contracts from disabled contract list, return a newly updated list
func filterContracts(disabledContractList []string, requestedContractList []string) []string {
	var updatedContractList []string

	// return the original list if no contracts have been requested
	if len(requestedContractList) == 0 {
		return disabledContractList
	}

	m := make(map[string]int, len(requestedContractList))
	for _, cn := range requestedContractList {
		m[cn] = 1
	}

	// populate the updatedContractList
	for _, cn := range disabledContractList {
		_, found := m[cn]
		if !found {
			updatedContractList = append(updatedContractList, cn)
		}
	}

	return updatedContractList
}

// helper method to fetch the disabled contract list from genesis config file
// if not initialized or from the database otherwise
func (r *ContractManagerRuntime) fetchDisabledContractList(txSimContext protocol.TxSimContext) ([]string, error) {
	// try to get disabled contract list from database
	disabledContractListBytes, err := txSimContext.Get(ContractName, []byte(keyContractName))
	if err != nil {
		return nil, err
	}

	// if the config file does not exist in the database yet, try fetch it from the genesis config file and store it
	// to the database
	if disabledContractListBytes == nil {
		disabledContractListBytes, err = r.initializeDisabledNativeContractList(txSimContext)
		if err != nil {
			r.log.Error(err)
			return nil, err
		}
	}

	var disabledContractList []string
	err = json.Unmarshal(disabledContractListBytes, &disabledContractList)
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

func (r *ContractManagerRuntime) initializeDisabledNativeContractList(
	txSimContext protocol.TxSimContext) ([]byte, error) {
	var (
		err                       error
		chainConfig               *configPb.ChainConfig
		disabledContractListBytes []byte
	)

	// 1. fetch chainConfig from genesis config file
	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}

	disabledContractList := chainConfig.DisabledNativeContract
	if disabledContractList == nil {
		disabledContractList = make([]string, 0)
	}
	disabledContractListBytes, err = json.Marshal(disabledContractList)
	if err != nil {
		return nil, err
	}

	// 2. store the disabledContractList field to the database
	err = storeDisabledContractList(txSimContext, disabledContractListBytes)
	if err != nil {
		return nil, err
	}

	return disabledContractListBytes, nil
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

// installContract 安装新合约
// 返回合约地址和基础信息
// @return name 合约名称
// @return Version 版本
// @return RuntimeType 运行时类型
// @return Status 状态一般是0
// @return Creator 创建合约的人
// @return Address 合约地址
// 注意：此处缺少合约本身的执行结果，可以发送事件
func (r *ContractManagerRuntime) installContract(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	name, version, byteCode, runtimeType, err := r.parseParam(parameters)
	if err != nil {
		return nil, err
	}
	contract, err := r.InstallContract(txSimContext, name, version, byteCode, runtimeType, parameters)
	if err != nil {
		return nil, err
	}
	r.log.Infof("install contract success[name:%s version:%s runtimeType:%d byteCodeLen:%d]", contract.Name,
		contract.Version, contract.RuntimeType, len(byteCode))
	return contract.Marshal()
}

// upgradeContract 升级现有合约
// 返回合约地址和基础信息
// @return name 合约名称
// @return Version 版本
// @return RuntimeType 运行时类型
// @return Status 状态一般是0
// @return Creator 创建合约的人
// @return Address 合约地址
// 注意：此处缺少合约本身的执行结果，可以发送事件
func (r *ContractManagerRuntime) upgradeContract(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	name, version, byteCode, runtimeType, err := r.parseParam(parameters)
	if err != nil {
		return nil, err
	}
	contract, err := r.UpgradeContract(txSimContext, name, version, byteCode, runtimeType, parameters)
	if err != nil {
		return nil, err
	}
	r.log.Infof("upgrade contract success[name:%s version:%s runtimeType:%d byteCodeLen:%d]", contract.Name,
		contract.Version, contract.RuntimeType, len(byteCode))
	return contract.Marshal()
}

// parseParam 参数转换
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

// freezeContract 冻结合约
// 返回合约地址和基础信息
// @return name 合约名称
// @return Version 版本
// @return RuntimeType 运行时类型
// @return Status 状态一般是0
// @return Creator 创建合约的人
// @return Address 合约地址
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

// unfreezeContract 解冻合约
// 返回合约地址和基础信息
// @return name 合约名称
// @return Version 版本
// @return RuntimeType 运行时类型
// @return Status 状态一般是0
// @return Creator 创建合约的人
// @return Address 合约地址
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

// revokeContract 吊销合约
// 返回合约地址和基础信息
// @return name 合约名称
// @return Version 版本
// @return RuntimeType 运行时类型
// @return Status 状态一般是0
// @return Creator 创建合约的人
// @return Address 合约地址
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
// 返回合约地址和基础信息
// @return name 合约名称
// @return Version 版本
// @return RuntimeType 运行时类型
// @return Status 状态一般是0
// @return Creator 创建合约的人
// @return Address 合约地址
func (r *ContractManagerRuntime) GetContractInfo(context protocol.TxSimContext, name string) (*commonPb.Contract,
	error) {
	if utils.IsAnyBlank(name) {
		err := fmt.Errorf("%s, param[contract_name] of get contract not found", common.ErrParams.Error())
		r.log.Warnf(err.Error())
		return nil, err
	}
	return context.GetContractByName(name)
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
// 返回合约地址和基础信息
// @return name 合约名称
// @return Version 版本
// @return RuntimeType 运行时类型
// @return Status 状态一般是0
// @return Creator 创建合约的人
// @return Address 合约地址
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

// InstallContract 安装新合约
func (r *ContractManagerRuntime) InstallContract(context protocol.TxSimContext, name, version string, byteCode []byte,
	runTime commonPb.RuntimeType, initParameters map[string][]byte) (*commonPb.Contract, error) {
	if !utils.CheckContractNameFormat(name) {
		return nil, errInvalidContractName
	}
	if runTime == commonPb.RuntimeType_EVM && !utils.CheckEvmAddressFormat(name) {
		return nil, errInvalidEvmContractName
	}
	key := utils.GetContractDbKey(name)
	//check name exist
	existContract, _ := context.Get(ContractName, key)
	if len(existContract) > 0 { //exist
		return nil, errContractExist
	}
	creator := r.getCreator(context)
	contract := &commonPb.Contract{
		Name:        name,
		Version:     version,
		RuntimeType: runTime,
		Status:      commonPb.ContractStatus_NORMAL,
		Creator:     creator,
	}
	cdata, _ := contract.Marshal()

	err := context.Put(ContractName, key, cdata)
	if err != nil {
		return nil, err
	}
	byteCodeKey := utils.GetContractByteCodeDbKey(name)
	err = context.Put(ContractName, byteCodeKey, byteCode)
	if err != nil {
		return nil, err
	}
	//实例化合约，并init合约，产生读写集
	result, _, statusCode := context.CallContract(nil, contract, protocol.ContractInitMethod, byteCode, initParameters,
		0, commonPb.TxType_INVOKE_CONTRACT)
	if statusCode != commonPb.TxStatusCode_SUCCESS {
		return nil, fmt.Errorf("%s, %s", errContractInitFail, result.Message)
	}
	if result.Code > 0 { //throw error
		return nil, fmt.Errorf("%s, %s", errContractInitFail, result.Message)
	}
	if runTime == commonPb.RuntimeType_EVM {
		//save bytecode body
		//EVM的特殊处理，在调用构造函数后会返回真正需要存的字节码，这里将之前的字节码覆盖
		if len(result.Result) > 0 {
			err := context.Put(ContractName, byteCodeKey, result.Result)
			if err != nil {
				return nil, fmt.Errorf("%s, %s", errContractInitFail, err)
			}
		}
	}
	return contract, nil
}

// UpgradeContract 升级现有合约
func (r *ContractManagerRuntime) UpgradeContract(context protocol.TxSimContext, name, version string, byteCode []byte,
	runTime commonPb.RuntimeType, upgradeParameters map[string][]byte) (*commonPb.Contract, error) {
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
	if contract.Version == version {
		return nil, errContractVersionExist
	}
	contract.RuntimeType = runTime
	contract.Version = version
	//update ContractInfo
	cdata, _ := contract.Marshal()
	err = context.Put(ContractName, key, cdata)
	if err != nil {
		return nil, err
	}
	//update Contract Bytecode
	byteCodeKey := utils.GetContractByteCodeDbKey(name)
	err = context.Put(ContractName, byteCodeKey, byteCode)
	if err != nil {
		return nil, err
	}
	//运行新合约的upgrade方法，产生读写集
	result, _, statusCode := context.CallContract(nil, contract, protocol.ContractUpgradeMethod, byteCode, upgradeParameters,
		0, commonPb.TxType_INVOKE_CONTRACT)
	if statusCode != commonPb.TxStatusCode_SUCCESS {
		return nil, fmt.Errorf("%s, %s", errContractUpgradeFail, result.Message)

	}
	if result.Code > 0 { //throw error
		return nil, fmt.Errorf("%s, %s", errContractUpgradeFail, result.Message)
	}
	if runTime == commonPb.RuntimeType_EVM {
		//save bytecode body
		//EVM的特殊处理，在调用构造函数后会返回真正需要存的字节码，这里将之前的字节码覆盖
		if len(result.Result) > 0 {
			err := context.Put(ContractName, byteCodeKey, result.Result)
			if err != nil {
				return nil, fmt.Errorf("%s, %s", errContractUpgradeFail, err)
			}
		}
	}
	return contract, nil
}

// FreezeContract comment at next version
func (r *ContractManagerRuntime) FreezeContract(context protocol.TxSimContext, name string) (
	*commonPb.Contract, error) {
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
	contract, err := context.GetContractByName(name)
	if err != nil {
		return nil, err
	}
	if contract.Status != commonPb.ContractStatus_NORMAL && contract.Status != commonPb.ContractStatus_FROZEN {
		r.log.Warnf("contract[%s] expect status:NORMAL or FROZEN,actual status:%s",
			name, contract.Status.String())
		return nil, errContractStatusInvalid
	}
	contract.Status = commonPb.ContractStatus_REVOKED
	key := utils.GetContractDbKey(name)
	cdata, _ := contract.Marshal()
	err = context.Put(ContractName, key, cdata)
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
	contract, err := context.GetContractByName(name)
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
	key := utils.GetContractDbKey(name)
	cdata, _ := contract.Marshal()
	err = context.Put(ContractName, key, cdata)
	if err != nil {
		return nil, err
	}
	return contract, nil
}

func (r *ContractManagerRuntime) getCreator(context protocol.TxSimContext) *accesscontrol.MemberFull {
	sender := context.GetSender()
	creator := &accesscontrol.MemberFull{
		OrgId:      sender.GetOrgId(),
		MemberType: sender.GetMemberType(),
		MemberInfo: sender.GetMemberInfo(),
	}
	return creator
}
