package chainconfigmgr

import (
	"errors"

	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
)

// UPMRuntime upgrade permission manager
type UPMRuntime struct {
	log protocol.Logger
}

func (r *UPMRuntime) enableOnlyCreatorUpgrade(txSimContext protocol.TxSimContext,
	_ map[string][]byte) ([]byte, error) {
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}
	if chainConfig.Contract.OnlyCreatorCanUpgrade {
		return nil, errors.New("already enabled")
	}
	chainConfig.Contract.OnlyCreatorCanUpgrade = true
	return SetChainConfig(txSimContext, chainConfig)
}

func (r *UPMRuntime) disableOnlyCreatorUpgrade(txSimContext protocol.TxSimContext,
	_ map[string][]byte) ([]byte, error) {
	chainConfig, err := common.GetChainConfig(txSimContext)
	if err != nil {
		return nil, err
	}
	if !chainConfig.Contract.OnlyCreatorCanUpgrade {
		return nil, errors.New("already disabled")
	}
	chainConfig.Contract.OnlyCreatorCanUpgrade = false
	return SetChainConfig(txSimContext, chainConfig)
}
