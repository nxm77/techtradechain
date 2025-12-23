/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package contractmgr is package for contract
package contractmgr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"

	"github.com/google/uuid"
)

const (
	// zxl address format
	zxlAddrPrefix = "ZX"
	// Version Compatibility 231
	blockVersion231  = 2030100
	blockVersion2312 = 2030102
	blockVersion235  = 2030500
	blockVersion237  = 2030700
)

var (
	// ContractName 当前合约的合约名
	ContractName = syscontract.SystemContract_CONTRACT_MANAGE.String()
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

//func generateAddress(context protocol.TxSimContext, name string) string {
//	var addr string
//	chainCfg, _ := context.GetBlockchainStore().GetLastChainConfig()
//	if chainCfg.Vm.AddrType == config.AddrType_ZXL {
//		addr, _ = evmutils.ZXAddress([]byte(name))
//		addr = addr[2:]
//	} else {
//		bytesAddr := evmutils.Keccak256([]byte(name))
//		addr = hex.EncodeToString(bytesAddr)[24:]
//	}
//
//	return addr
//}

// checkAndAddZXPrefix add zxl address prefix for contract
func checkAndAddZXPrefix(txSimContext protocol.TxSimContext, contract *commonPb.Contract) *commonPb.Contract {
	cfg, _ := txSimContext.GetBlockchainStore().GetLastChainConfig()

	//if contract.Address != 40, is invalid address
	if txSimContext.GetBlockVersion() < 2300 || cfg.Vm.AddrType != config.AddrType_ZXL || len(contract.Address) != 40 {
		return contract
	}

	contract.Address = zxlAddrPrefix + contract.Address
	return contract
}

//truncateZXPrefixForContractAddr delete zxl address prefix
//func truncateZXPrefixForContractAddr(contract *commonPb.Contract) *commonPb.Contract {
//	if contract.Address[0:2] == zxlAddrPrefix {
//		contract.Address = contract.Address[2:]
//	}
//
//	return contract
//}

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

// enable access to a native contract
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

// verifyContractAccess verify if access to the requested contract is enabled according to the disabled contract list
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

// getContractInfo 查询合约的信息
func (r *ContractManagerRuntime) getContractInfo(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	name := string(parameters[syscontract.GetContractInfo_CONTRACT_NAME.String()])
	cfg := txSimContext.GetLastChainConfig()

	if cfg.Vm.AddrType == config.AddrType_ZXL && txSimContext.GetBlockVersion() >= 2300 &&
		utils.CheckZxlAddrFormat(name) {
		name = name[2:]
	}

	contract, err := r.GetContractInfo(txSimContext, name)
	if err != nil {
		return nil, err
	}

	// RuntimeType_GO is not exposed in v230
	if contract.RuntimeType == commonPb.RuntimeType_GO {
		contract.RuntimeType = commonPb.RuntimeType_DOCKER_GO
	}

	contract = checkAndAddZXPrefix(txSimContext, contract)
	return json.Marshal(contract)
}

// getAllContracts 获得所有合约的信息
func (r *ContractManagerRuntime) getAllContracts(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	contracts, err := r.GetAllContracts(txSimContext)
	if err != nil {
		return nil, err
	}
	// RuntimeType_GO is not exposed in v230
	for _, contract := range contracts {
		if contract.RuntimeType == commonPb.RuntimeType_GO {
			contract.RuntimeType = commonPb.RuntimeType_DOCKER_GO
		}

		checkAndAddZXPrefix(txSimContext, contract)
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
func (r *ContractManagerRuntime) installContract(txSimContext protocol.TxSimContext,
	parameters map[string][]byte) *commonPb.ContractResult {
	name, version, byteCode, runtimeType, err := r.parseParam(parameters)
	if err != nil {
		return common.ResultError(err)
	}

	if !utils.CheckContractNameFormat(name) {
		return common.ResultError(errInvalidContractName)
	}

	if txSimContext.GetBlockVersion() < 2220 {
		if runtimeType == commonPb.RuntimeType_EVM && !utils.CheckEvmAddressFormat(name) {
			return common.ResultError(errInvalidEvmContractName)
		}
	}

	if txSimContext.GetBlockVersion() > 2230 {
		if runtimeType < commonPb.RuntimeType_NATIVE || runtimeType > commonPb.RuntimeType_GO {
			return common.ResultError(fmt.Errorf("invalid runtime type[%v]", runtimeType))
		}

		if byteCode == nil {
			return common.ResultError(fmt.Errorf("bytecode is nil"))
		}
	}

	if txSimContext.GetBlockVersion() < 2300 {
		contract, gas, e := r.installLT2300(txSimContext, name, version, byteCode, runtimeType, parameters)
		if e != nil {
			return common.ResultError(e)
		}

		r.log.Infof("install contract success[name:%s version:%s runtimeType:%d byteCodeLen:%d]", contract.Name,
			contract.Version, contract.RuntimeType, len(byteCode))

		contractBytes, _ := contract.Marshal()
		return common.ResultSuccess(contractBytes, gas)
	}

	contract, result, err := r.install(txSimContext, name, version, byteCode, runtimeType, parameters)
	if err != nil {
		return common.ResultError(err)
	}

	contract = checkAndAddZXPrefix(txSimContext, contract)
	r.log.Infof("install contract success[name:%s version:%s runtimeType:%d byteCodeLen:%d]", contract.Name,
		contract.Version, contract.RuntimeType, len(byteCode))

	contractBytes, _ := contract.Marshal()
	result.Result = contractBytes
	return result
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
func (r *ContractManagerRuntime) upgradeContract(txSimContext protocol.TxSimContext,
	parameters map[string][]byte) *commonPb.ContractResult {
	name, version, byteCode, runtimeType, err := r.parseParam(parameters)
	if err != nil {
		return common.ResultError(err)
	}

	contract, err := getContractByName(txSimContext, name)
	if err != nil {
		return common.ResultError(err)
	}

	if contract.Version == version {
		return common.ResultError(errContractVersionExist)
	}

	// 新特性： 主链版；安装合约只允许本人升级
	if txSimContext.GetBlockVersion() >= blockVersion2312 {
		chainConfig := txSimContext.GetLastChainConfig()
		if chainConfig.Contract.OnlyCreatorCanUpgrade {
			var sender *accesscontrol.MemberFull
			sender, err = r.getSender(txSimContext)
			if err != nil {
				return common.ResultError(err)
			}
			if !strings.EqualFold(sender.Uid, contract.Creator.Uid) {
				return common.ResultError(errors.New("only creators are allowed to upgrade contract"))
			}
		}
	}
	if txSimContext.GetBlockVersion() < blockVersion231 {
		if contract.Status != commonPb.ContractStatus_NORMAL {
			return common.ResultError(errContractStatusInvalid)
		}
	} else {
		if contract.Status == commonPb.ContractStatus_REVOKED {
			return common.ResultError(errContractStatusInvalid)
		}
	}

	contract.RuntimeType = runtimeType
	contract.Version = version
	if txSimContext.GetBlockVersion() >= 2220 && len(contract.Address) == 0 {
		//contract.Address = generateAddress(context, name)
		chainCfg, _ := txSimContext.GetBlockchainStore().GetLastChainConfig()
		addr, err := utils.NameToAddrStr(name, chainCfg.Vm.AddrType, txSimContext.GetBlockVersion())
		if err != nil {
			return common.ResultError(err)
		}
		contract.Address = addr
	}

	if txSimContext.GetBlockVersion() < 2300 {
		c1, gas, err1 := r.upgradeLT2300(txSimContext, contract, byteCode, parameters)
		if err1 != nil {
			return common.ResultError(err1)
		}
		r.log.Infof("upgrade contract success[name:%s version:%s runtimeType:%d byteCodeLen:%d]", c1.Name,
			c1.Version, c1.RuntimeType, len(byteCode))
		contractBytes, _ := c1.Marshal()
		return common.ResultSuccess(contractBytes, gas)
	}

	if txSimContext.GetBlockVersion() >= blockVersion235 {
		// 安装和升级合约交易，单独出块，不支持并行
		contract.Index++
	}

	c2, result, err2 := r.upgrade(txSimContext, contract, byteCode, parameters)
	if err2 != nil {
		return common.ResultError(err2)
	}

	c2 = checkAndAddZXPrefix(txSimContext, c2)
	r.log.Infof("upgrade contract success[name:%s version:%s runtimeType:%d byteCodeLen:%d]", c2.Name,
		c2.Version, c2.RuntimeType, len(byteCode))
	contractBytes, _ := c2.Marshal()
	result.Result = contractBytes
	return result
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

	contract = checkAndAddZXPrefix(txSimContext, contract)
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

	contract = checkAndAddZXPrefix(txSimContext, contract)
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

	contract = checkAndAddZXPrefix(txSimContext, contract)
	r.log.Infof("revoke contract success[name:%s version:%s runtimeType:%d]", contract.Name, contract.Version,
		contract.RuntimeType)
	return json.Marshal(contract)
}

// ContractManagerRuntime contract manager runtime instance
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
	return getContractByName(context, name)
}

// GetContractByteCode 获得合约的字节码
func (r *ContractManagerRuntime) GetContractByteCode(context protocol.TxSimContext, name string) ([]byte, error) {
	if utils.IsAnyBlank(name) {
		err := fmt.Errorf("%s, param[contract_name] of get contract not found", common.ErrParams.Error())
		r.log.Warnf(err.Error())
		return nil, err
	}
	return context.GetContractBytecode(name)
}

func findContractInSlice(contractList []*commonPb.Contract, target *commonPb.Contract) bool {
	for _, c := range contractList {
		if c.Name == target.Name {
			return true
		}
	}

	return false
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

		if context.GetBlockVersion() < 2300 {
			result = append(result, contract)
		} else {
			if !findContractInSlice(result, contract) {
				result = append(result, contract)
			}
		}
	}
	return result, nil
}

// saveContract 存储新合约
func (r *ContractManagerRuntime) saveContract(context protocol.TxSimContext, name, version string, byteCode []byte,
	runTime commonPb.RuntimeType, index uint32) (*commonPb.Contract, []byte, []byte, error) {
	nameKey := utils.GetContractDbKey(name)
	existContract, _ := context.Get(ContractName, nameKey)
	if len(existContract) > 0 { //exist
		return nil, nil, nil, errContractExist
	}

	creator, err := r.getSender(context)
	if err != nil {
		return nil, nil, nil, err
	}

	contract := &commonPb.Contract{
		Name:        name,
		Version:     version,
		RuntimeType: runTime,
		Status:      commonPb.ContractStatus_NORMAL,
		Creator:     creator,
	}

	// add creating information in contract information
	if context.GetBlockVersion() >= blockVersion237 {
		contract.InstallCreateBlockHeight = context.GetBlockHeight()
		contract.InstallCreateTxId = context.GetTx().GetPayload().GetTxId()
		contract.InstallInitBlockHeight = context.GetBlockHeight()
		contract.InstallInitTxId = context.GetTx().GetPayload().GetTxId()
	}

	// 这个自增的index 用来区分不同版本的合约，不再依赖用户自己定义的version字符串来做唯一标识
	if context.GetBlockVersion() >= blockVersion235 {
		contract.Index = index
	}

	if context.GetBlockVersion() >= 2220 {
		//contract.Address = generateAddress(context, name)
		chainCfg, _ := context.GetBlockchainStore().GetLastChainConfig()
		addr, err1 := utils.NameToAddrStr(name, chainCfg.Vm.AddrType, context.GetBlockVersion())
		if err1 != nil {
			return nil, nil, nil, err1
		}
		contract.Address = addr
	}

	// 1. contract bin version >= 2300 && RuntimeType = RuntimeType_DOCKER_GO (old): replace to RuntimeType_GO (new)
	//		CallContract with extracted bytecode
	// 2. (not enabled yet)contract bin version >= 2300 && RuntimeType = RuntimeType_GO (new)
	//		CallContract with extracted bytecode
	// 3. CallContract with archived bytecode
	if runTime == commonPb.RuntimeType_GO {
		return nil, nil, nil, fmt.Errorf("unknown runtime type, %v", err)
	}
	var extBytecode []byte
	if context.GetBlockVersion() >= 2300 && runTime == commonPb.RuntimeType_DOCKER_GO {
		extBytecode, err = r.checkGoContractVersion(contract, byteCode)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to check contract bin version, %v", err)
		}
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
		return nil, nil, nil, err
	}

	byteCodeKey := utils.GetContractByteCodeDbKey(name)
	err = context.Put(ContractName, byteCodeKey, byteCode)
	if err != nil {
		return nil, nil, nil, err
	}

	return contract, byteCodeKey, extBytecode, nil
}

// installLT2300 安装新合约, 版本号 < 2.3.0时使用
func (r *ContractManagerRuntime) installLT2300(context protocol.TxSimContext, name, version string, byteCode []byte,
	runTime commonPb.RuntimeType, initParameters map[string][]byte) (*commonPb.Contract, uint64, error) {

	contract, byteCodeKey, extBytecode, err := r.saveContract(context, name, version, byteCode, runTime, 0)
	if err != nil {
		return nil, 0, err
	}

	//实例化合约，并init合约，产生读写集
	callBytecode := byteCode
	// contract bin version >= 2.3.0 -> vm-engine
	// else: -> vm-docker-go
	if extBytecode != nil {
		callBytecode = extBytecode
	}

	var caller *commonPb.Contract
	caller, err = context.GetContractByName(syscontract.SystemContract_CONTRACT_MANAGE.String())
	if err != nil {
		return nil, 0, err
	}

	result, _, statusCode := context.CallContract(
		caller,
		contract,
		protocol.ContractInitMethod,
		callBytecode,
		initParameters,
		0,
		commonPb.TxType_INVOKE_CONTRACT,
	)
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

func differentiateErr(version uint32, rtType commonPb.RuntimeType, msg string) (*commonPb.Contract,
	*commonPb.ContractResult, error) {
	if version < 2218 || version == 2300 || version == 2030100 {
		return nil, nil, fmt.Errorf("%s, %s", errContractInitFail, msg)
	}

	if rtType == commonPb.RuntimeType_EVM {
		return nil, nil, errors.New(msg)
	}
	return nil, nil, fmt.Errorf("%s, %s", errContractInitFail, msg)
}

// install 安装新合约
func (r *ContractManagerRuntime) install(context protocol.TxSimContext, name, version string, byteCode []byte,
	runTime commonPb.RuntimeType, initParameters map[string][]byte) (*commonPb.Contract, *commonPb.ContractResult, error) {

	var contractIndex uint32
	if context.GetBlockVersion() >= blockVersion235 {
		contractIndex = 1
	}
	contract, byteCodeKey, extBytecode, err := r.saveContract(context, name, version, byteCode, runTime, contractIndex)
	if err != nil {
		return nil, nil, err
	}

	//实例化合约，并init合约，产生读写集
	callBytecode := byteCode
	// contract bin version >= 2.3.0 -> vm-engine
	// else: -> vm-docker-go
	if extBytecode != nil {
		callBytecode = extBytecode
	}

	var caller *commonPb.Contract
	blockVersion := context.GetBlockVersion()
	caller, err = context.GetContractByName(syscontract.SystemContract_CONTRACT_MANAGE.String())
	if err != nil {
		//return nil, nil, fmt.Errorf("%s, %s", errContractInitFail, err.Error())
		return differentiateErr(blockVersion, runTime, err.Error())
	}

	result, _, statusCode := context.CallContract(
		caller,
		contract,
		protocol.ContractInitMethod,
		callBytecode,
		initParameters,
		0,
		commonPb.TxType_INVOKE_CONTRACT,
	)
	r.log.Debugf("【gas calc】%v, contract_manager::install => gasUsed = %v",
		context.GetTx().Payload.TxId, result.GasUsed)

	if statusCode != commonPb.TxStatusCode_SUCCESS {
		r.log.Errorf("install contract %s:%s, failed: %s", name, version, string(result.Result))
		return differentiateErr(blockVersion, runTime, result.Message)
	}
	if result.Code > 0 { //throw error
		r.log.Errorf("install contract %s:%s, failed: %s", name, version, result.Result)
		return differentiateErr(blockVersion, runTime, result.Message)
	}
	if runTime == commonPb.RuntimeType_EVM {
		//save bytecode body
		//EVM的特殊处理，在调用构造函数后会返回真正需要存的字节码，这里将之前的字节码覆盖
		if len(result.Result) > 0 {
			err := context.Put(ContractName, byteCodeKey, result.Result)
			if err != nil {
				//return nil, nil, fmt.Errorf("%s, %s", errContractInitFail, err)
				return differentiateErr(blockVersion, runTime, err.Error())
			}
		}
	}
	return contract, result, nil
}

// getSender 从Context获得合约的创建人
func (r *ContractManagerRuntime) getSender(txSimContext protocol.TxSimContext) (*accesscontrol.MemberFull, error) {
	return common.GetSenderMemberFull(txSimContext, r.log)
}

// upgradeLT2300 upgrade contract when version < 2300
func (r *ContractManagerRuntime) upgradeLT2300(context protocol.TxSimContext, contract *commonPb.Contract,
	byteCode []byte, upgradeParameters map[string][]byte) (*commonPb.Contract, uint64, error) {
	upgradeParameters["__upgrade_contract_old_contract_name"] = []byte(contract.Name)
	upgradeParameters["__upgrade_contract_old_contract_version"] = []byte(contract.Version)
	upgradeParameters["__upgrade_contract_old_contract_runtime_type"] = []byte(contract.RuntimeType.String())

	err := putContract(context, contract)
	if err != nil {
		return nil, 0, err
	}

	//update Contract Bytecode
	byteCodeKey := utils.GetContractByteCodeDbKey(contract.Name)
	err = context.Put(ContractName, byteCodeKey, byteCode)
	if err != nil {
		return nil, 0, err
	}

	var caller *commonPb.Contract
	caller, err = context.GetContractByName(syscontract.SystemContract_CONTRACT_MANAGE.String())
	if err != nil {
		return nil, 0, err
	}

	result, _, statusCode := context.CallContract(caller, contract, protocol.ContractUpgradeMethod,
		byteCode, upgradeParameters, 0, commonPb.TxType_INVOKE_CONTRACT)
	if statusCode != commonPb.TxStatusCode_SUCCESS {
		return nil, 0, fmt.Errorf("%s, %s", errContractUpgradeFail, result.Message)

	}

	if result.Code > 0 { //throw error
		return nil, 0, fmt.Errorf("%s, %s", errContractUpgradeFail, result.Message)
	}

	if contract.RuntimeType == commonPb.RuntimeType_EVM {
		//save bytecode body
		if len(result.Result) > 0 {
			err := context.Put(ContractName, byteCodeKey, result.Result)
			if err != nil {
				return nil, 0, fmt.Errorf("%s, %s", errContractUpgradeFail, err)
			}
		}
	}

	return contract, result.GasUsed, nil
}

// upgrade upgrade contract when version >= 2300
func (r *ContractManagerRuntime) upgrade(context protocol.TxSimContext, contract *commonPb.Contract,
	byteCode []byte, upgradeParameters map[string][]byte) (*commonPb.Contract, *commonPb.ContractResult, error) {
	// 1. contract bin version >= 2300 && RuntimeType = RuntimeType_DOCKER_GO (old): replace to RuntimeType_GO (new),
	// then store contract bin instead of 7z file
	// 2. (not enabled yet)contract bin version >= 2300 && RuntimeType = RuntimeType_GO (new),
	// store contract bin instead of 7z file
	if contract.RuntimeType == commonPb.RuntimeType_GO {
		return nil, nil, fmt.Errorf("unknown runtime type: %v", commonPb.RuntimeType_GO)
	}
	var extBytecode []byte
	var err error
	if contract.RuntimeType == commonPb.RuntimeType_DOCKER_GO {
		extBytecode, err = r.checkGoContractVersion(contract, byteCode)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to check contract bin version, %v", err)
		}
	}

	//实例化合约，并init合约，产生读写集
	callBytecode := byteCode
	// contract bin version >= 2.3.0 -> vm-engine
	// else: -> vm-docker-go
	if extBytecode != nil {
		callBytecode = extBytecode
	}

	upgradeParameters["__upgrade_contract_old_contract_name"] = []byte(contract.Name)
	upgradeParameters["__upgrade_contract_old_contract_version"] = []byte(contract.Version)
	upgradeParameters["__upgrade_contract_old_contract_runtime_type"] = []byte(contract.RuntimeType.String())

	// add upgrading information in contract information
	if context.GetBlockVersion() >= blockVersion237 {
		contract.UpgradeCreateBlockHeight = context.GetBlockHeight()
		contract.UpgradeCreateTxId = context.GetTx().GetPayload().GetTxId()
		contract.UpgradeInitBlockHeight = context.GetBlockHeight()
		contract.UpgradeInitTxId = context.GetTx().GetPayload().GetTxId()
	}

	err = putContract(context, contract)
	if err != nil {
		return nil, nil, err
	}

	//update Contract Bytecode
	byteCodeKey := utils.GetContractByteCodeDbKey(contract.Name)
	err = context.Put(ContractName, byteCodeKey, byteCode)
	if err != nil {
		return nil, nil, err
	}

	var caller *commonPb.Contract
	caller, err = context.GetContractByName(syscontract.SystemContract_CONTRACT_MANAGE.String())
	if err != nil {
		return nil, nil, fmt.Errorf("%s, %s", errContractUpgradeFail, err.Error())
	}

	result, _, statusCode := context.CallContract(caller, contract, protocol.ContractUpgradeMethod,
		callBytecode, upgradeParameters, 0, commonPb.TxType_INVOKE_CONTRACT)
	if statusCode != commonPb.TxStatusCode_SUCCESS {
		r.log.Errorf("upgrade contract %s:%s, failed: %s", contract.Name, contract.Version, result.Result)
		return nil, nil, fmt.Errorf("%s, %s", errContractUpgradeFail, result.Message)

	}

	if result.Code > 0 { //throw error
		r.log.Errorf("upgrade contract %s:%s, failed: %s", contract.Name, contract.Version, result.Result)
		return nil, nil, fmt.Errorf("%s, %s", errContractUpgradeFail, result.Message)
	}

	if contract.RuntimeType == commonPb.RuntimeType_EVM && len(result.Result) > 0 {
		//save bytecode body
		err := context.Put(ContractName, byteCodeKey, result.Result)
		if err != nil {
			return nil, nil, fmt.Errorf("%s, %s", errContractUpgradeFail, err)
		}
	}
	return contract, result, nil
}

// FreezeContract 冻结合约
func (r *ContractManagerRuntime) FreezeContract(context protocol.TxSimContext, name string) (
	*commonPb.Contract, error) {
	if name == syscontract.SystemContract_CONTRACT_MANAGE.String() { //合约管理合约不能被冻结
		r.log.Warnf("cannot freeze special contract:%s", name)
		return nil, errInvalidContractName
	}
	return r.changeContractStatus(context, name, commonPb.ContractStatus_NORMAL, commonPb.ContractStatus_FROZEN)
}

// UnfreezeContract 解冻合约
func (r *ContractManagerRuntime) UnfreezeContract(context protocol.TxSimContext, name string) (
	*commonPb.Contract, error) {
	return r.changeContractStatus(context, name, commonPb.ContractStatus_FROZEN, commonPb.ContractStatus_NORMAL)
}

// RevokeContract 吊销合约
func (r *ContractManagerRuntime) RevokeContract(context protocol.TxSimContext, name string) (
	*commonPb.Contract, error) {
	if utils.IsAnyBlank(name) {
		err := fmt.Errorf("%s, param[contract_name] not found", common.ErrParams.Error())
		r.log.Warnf(err.Error())
		return nil, err
	}
	//if name == syscontract.SystemContract_CONTRACT_MANAGE.String() || //某些合约是不能被禁用的
	//	name == syscontract.SystemContract_CHAIN_CONFIG.String() {
	//	r.log.Warnf("cannot revoke special contract:%s", name)
	//	return nil, errInvalidContractName
	//}
	contract, err := getContractByName(context, name)
	if err != nil {
		return nil, err
	}

	if contract.RuntimeType == commonPb.RuntimeType_NATIVE {
		//系统合约不能被吊销
		r.log.Warnf("cannot revoke special contract:%s", name)
		return nil, errInvalidContractName
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
	existContract, err := context.Get(ContractName, key)
	if err != nil || len(existContract) == 0 { //not exist
		return nil, errContractNotExist
	}
	contract := &commonPb.Contract{}
	err = contract.Unmarshal(existContract)
	if err != nil {
		return nil, err
	}
	return contract, nil
}

// changeContractStatus 改变合约的状态
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

// InitNewNativeContract 初始化系统合约，之前没有初始化的补齐
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
	chainCfg, _ := txSimContext.GetBlockchainStore().GetLastChainConfig()
	//	chainCfg, _ := common.GetChainConfigNoRecord(txSimContext) //Suggest:这样取更好

	syscontractKeys := []int{}
	for k := range syscontract.SystemContract_name {
		syscontractKeys = append(syscontractKeys, int(k))
	}
	sort.Ints(syscontractKeys)
	for _, k := range syscontractKeys {
		cname := syscontract.SystemContract_name[int32(k)]
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
			addr, err := utils.NameToAddrStr(cname, chainCfg.Vm.AddrType, txSimContext.GetBlockVersion())
			if err != nil {
				return nil, err
			}

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

func (r *ContractManagerRuntime) checkGoContractVersion(contract *commonPb.Contract, byteCode []byte) ([]byte, error) {

	// tmp contract dir (include .7z and bin files)
	tmpContractDir := "tmp-contract-" + uuid.New().String()

	// contract zip path (.7z path)
	contractZipPath := filepath.Join(tmpContractDir, fmt.Sprintf("%s.7z", contract.Name))

	// save bytecode to tmpContractDir
	var err error
	err = saveBytesToDisk(byteCode, contractZipPath, tmpContractDir)
	if err != nil {
		return nil, fmt.Errorf("failed to save tmp contract bin for version query")
	}

	// extract 7z file
	unzipCommand := fmt.Sprintf("7z e %s -o%s -y", contractZipPath, tmpContractDir) // contract1
	err = runCmd(unzipCommand)
	if err != nil {
		if strings.Contains(err.Error(), tmpContractDir) {
			return nil, fmt.Errorf("failed to extract contract")
		}
		return nil, fmt.Errorf("failed to extract contract, %v", err)
	}

	// remove tmpContractDir in the end
	defer func() {
		if err = os.RemoveAll(tmpContractDir); err != nil {
			r.log.Errorf("failed to remove tmp contract bin dir %s", tmpContractDir)
		}
	}()

	// exec contract bin to get version
	// read all files in tmpContractDir
	fileInfoList, err := ioutil.ReadDir(tmpContractDir)
	if err != nil {
		r.log.Errorf("failed to read tmp contract dir %s, %v", tmpContractDir, err)
		if strings.Contains(err.Error(), tmpContractDir) {
			return nil, fmt.Errorf("failed to read tmp contract dir")
		}
		return nil, fmt.Errorf("failed to read tmp contract dir, %v", err)
	}

	// 2 files, include .7z file and bin file
	if len(fileInfoList) != 2 {
		r.log.Errorf("file num in contract dir %s != 2", tmpContractDir)
		return nil, errors.New("wrong contract 7z, extracted file num in contract dir != 2")
	}

	var extBytecode []byte
	// range file list
	for i := range fileInfoList {

		// skip .7z file
		if strings.HasSuffix(fileInfoList[i].Name(), ".7z") {
			continue
		}

		// get contract bin file path
		fp := filepath.Join(tmpContractDir, fileInfoList[i].Name())
		extBytecode, err = ioutil.ReadFile(fp)
		if err != nil {
			r.log.Errorf("read from byteCode file %s failed, %s", fp, err)
			if strings.Contains(err.Error(), tmpContractDir) {
				return nil, fmt.Errorf("read from byteCode file failed")
			}
			return nil, fmt.Errorf("read from byteCode file failed, %s", err)
		}

		//// new go engine contract, store contract bin bytecode
		//if contract.RuntimeType == commonPb.RuntimeType_GO {
		//	// replace 7z bytecode with contract bin bytecode, return
		//	return extBytecode, nil
		//}

		contractSDKVersion := "Contract_SDK_Version:fc5101a7f55d71e234242163fd1bdfaa4fdea7437bf161f7e1cf7c49e57580a2"
		// new contract bin(>= v2.3.0) with old runtime type
		if bytes.Contains(extBytecode, []byte(contractSDKVersion)) {
			contract.RuntimeType = commonPb.RuntimeType_GO

			// replace 7z bytecode with contract bin bytecode, return
			return extBytecode, nil
		}
		// old contract bin(< v2.3.0) with old runtime type
		return nil, nil
	}
	return nil, fmt.Errorf("no contract binaries satisfied")
}

// saveBytesToDisk save contract bytecode to disk
func saveBytesToDisk(bytes []byte, newFilePath, newFileDir string) error {

	if _, err := os.Stat(newFilePath); os.IsNotExist(err) {
		err = os.Mkdir(newFileDir, 0777)
		if err != nil {
			return err
		}
	}

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
