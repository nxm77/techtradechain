package chainconfigmgr2310

import (
	"errors"
	"strconv"

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
