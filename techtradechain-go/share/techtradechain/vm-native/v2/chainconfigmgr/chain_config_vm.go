package chainconfigmgr

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"techtradechain.com/techtradechain/common/v2/msgbus"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
)

// VmRuntime Vm config update
type VmRuntime struct {
	log protocol.Logger
}

// EnableOrDisableMultiSignManualRun set enable_manual_run flag by `multi_sign_enable_manual_run` key
func (r *VmRuntime) EnableOrDisableMultiSignManualRun(txSimContext protocol.TxSimContext,
	params map[string][]byte) (result []byte, err error) {

	var chainConfig *configPb.ChainConfig
	chainConfig, err = common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}

	if chainConfig.Vm == nil {
		return nil, errors.New("the Vm section in chain config is nil")

	} else if chainConfig.Vm.Native == nil {
		chainConfig.Vm.Native = &configPb.VmNative{
			Multisign: &configPb.MultiSign{
				EnableManualRun: false,
			},
		}
	} else if chainConfig.Vm.Native.Multisign == nil {
		chainConfig.Vm.Native.Multisign = &configPb.MultiSign{
			EnableManualRun: false,
		}
	}

	enableManualRun, ok := params[paramNameMultiSignEnableManualRun]
	if !ok {
		return nil, errors.New("the key `multi_sign_enable_manual_run` is not found in params")
	}

	parseBool, _ := strconv.ParseBool(string(enableManualRun))
	if chainConfig.Vm == nil {
		chainConfig.Vm = &configPb.Vm{}
	}
	if chainConfig.Vm.Native == nil {
		chainConfig.Vm.Native = &configPb.VmNative{}
	}
	if chainConfig.Vm.Native.Multisign == nil {
		chainConfig.Vm.Native.Multisign = &configPb.MultiSign{}
	}
	chainConfig.Vm.Native.Multisign.EnableManualRun = parseBool

	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Error(err)
		return nil, err
	}
	return result, nil
}

// VMSupportListAdd add vm support list
func (r *VmRuntime) VMSupportListAdd(txSimContext protocol.TxSimContext, params map[string][]byte) (
	[]byte, []*commonPb.ContractEvent, error) {
	// get current ChainConfig
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		r.log.Error(err)
		return nil, nil, err
	}
	// only lowercase letters are allowed
	vmType := strings.ToLower(strings.TrimSpace(string(params[paramVmType])))

	// verify vm type
	if !protocol.VerifyVmType(vmType) {
		return nil, nil, fmt.Errorf("vm type[%s] is not a valid vm type", vmType)
	}

	// if vm type already exists, return error
	for _, item := range chainConfig.Vm.SupportList {
		if strings.ToLower(item) == vmType {
			return nil, nil, fmt.Errorf("vm type[%s] already exist", vmType)
		}
	}

	// add the vm type to ChainConfig
	chainConfig.Vm.SupportList = append(chainConfig.Vm.SupportList, vmType)

	// set the new ChainConfig
	var result []byte
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("vm type add fail, [%s], %s", vmType, err.Error())
		return result, nil, err
	}

	// generate ChainConfig event
	event := &commonPb.ContractEvent{
		Topic:           strconv.Itoa(int(msgbus.ChainConfig)),
		TxId:            txSimContext.GetTx().Payload.TxId,
		ContractName:    chainConfigContractName,
		ContractVersion: chainConfig.Version,
		EventData:       []string{hex.EncodeToString(result)},
	}
	r.log.Infof("vm type add success. [%s]", vmType)

	return result, []*commonPb.ContractEvent{event}, err
}

// VMSupportListDel remove vm type in support list
func (r *VmRuntime) VMSupportListDel(txSimContext protocol.TxSimContext, params map[string][]byte) (
	[]byte, []*commonPb.ContractEvent, error) {
	// get current ChainConfig
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		r.log.Error(err)
		return nil, nil, err
	}
	// only lowercase letters are allowed
	vmType := strings.ToLower(strings.TrimSpace(string(params[paramVmType])))

	// find the vmType in current support list
	delIndex := -1
	for i, item := range chainConfig.Vm.SupportList {
		if strings.ToLower(item) == vmType {
			delIndex = i
			break
		}
	}

	// not found
	if delIndex == -1 {
		return nil, nil, fmt.Errorf("vm type[%s] not exist", vmType)
	}

	// remove the vmType
	chainConfig.Vm.SupportList = append(chainConfig.Vm.SupportList[:delIndex],
		chainConfig.Vm.SupportList[delIndex+1:]...)

	// set the new ChainConfig
	var result []byte
	result, err = SetChainConfig(txSimContext, chainConfig)
	if err != nil {
		r.log.Errorf("vm type delete fail, [%s], %s", vmType, err.Error())
		return result, nil, err
	}

	// generate ChainConfig event
	event := &commonPb.ContractEvent{
		Topic:           strconv.Itoa(int(msgbus.ChainConfig)),
		TxId:            txSimContext.GetTx().Payload.TxId,
		ContractName:    chainConfigContractName,
		ContractVersion: chainConfig.Version,
		EventData:       []string{hex.EncodeToString(result)},
	}
	r.log.Infof("vm type delete success. [%s]", vmType)

	return result, []*commonPb.ContractEvent{event}, err
}
