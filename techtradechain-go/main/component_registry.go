/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"techtradechain.com/techtradechain-go/module/consensus"
	"techtradechain.com/techtradechain-go/module/txpool"
	"techtradechain.com/techtradechain-go/module/vm"
	dpos "techtradechain.com/techtradechain/consensus-dpos/v2"
	maxbft "techtradechain.com/techtradechain/consensus-maxbft/v2"
	raft "techtradechain.com/techtradechain/consensus-raft/v2"
	solo "techtradechain.com/techtradechain/consensus-solo/v2"
	tbft "techtradechain.com/techtradechain/consensus-tbft/v2"
	utils "techtradechain.com/techtradechain/consensus-utils/v2"
	"techtradechain.com/techtradechain/localconf/v2"
	"techtradechain.com/techtradechain/logger/v2"
	consensusPb "techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/protocol/v2"
	batch "techtradechain.com/techtradechain/txpool-batch/v2"
	normal "techtradechain.com/techtradechain/txpool-normal/v2"
	single "techtradechain.com/techtradechain/txpool-single/v2"
	dockergo "techtradechain.com/techtradechain/vm-docker-go/v2"
	goEngine "techtradechain.com/techtradechain/vm-engine/v2"
	evm "techtradechain.com/techtradechain/vm-evm/v2"
	gasm "techtradechain.com/techtradechain/vm-gasm/v2"
	wasmer "techtradechain.com/techtradechain/vm-wasmer/v2"
	wxvm "techtradechain.com/techtradechain/vm-wxvm/v2"
)

func init() {
	// txPool
	txpool.RegisterTxPoolProvider(single.TxPoolType, single.NewTxPoolImpl)
	txpool.RegisterTxPoolProvider(normal.TxPoolType, normal.NewNormalPool)
	txpool.RegisterTxPoolProvider(batch.TxPoolType, batch.NewBatchTxPool)

	// vm
	vm.RegisterVmProvider(
		"GASM",
		func(chainId string, configs map[string]interface{}) (protocol.VmInstancesManager, error) {
			return &gasm.InstancesManager{}, nil
		})
	vm.RegisterVmProvider(
		"WASMER",
		func(chainId string, configs map[string]interface{}) (protocol.VmInstancesManager, error) {
			return wasmer.NewInstancesManager(chainId), nil
		})
	vm.RegisterVmProvider(
		"WXVM",
		func(chainId string, configs map[string]interface{}) (protocol.VmInstancesManager, error) {
			return &wxvm.InstancesManager{}, nil
		})
	vm.RegisterVmProvider(
		"EVM",
		func(chainId string, configs map[string]interface{}) (protocol.VmInstancesManager, error) {
			return &evm.InstancesManager{}, nil
		})

	// chainId string, logger protocol.Logger, vmConfig map[string]interface{}
	vm.RegisterVmProvider(
		"DOCKERGO",
		func(chainId string, configs map[string]interface{}) (protocol.VmInstancesManager, error) {
			return dockergo.NewDockerManager(
				chainId,
				localconf.TechTradeChainConfig.VMConfig.DockerVMGo,
			), nil
		})

	// chainId string, logger protocol.Logger, vmConfig map[string]interface{}
	vm.RegisterVmProvider(
		"GO",
		func(chainId string, configs map[string]interface{}) (protocol.VmInstancesManager, error) {
			return goEngine.NewInstancesManager(
				chainId,
				logger.GetLoggerByChain(logger.MODULE_VM, chainId),
				localconf.TechTradeChainConfig.VMConfig.Go,
			)
		})

	// consensus
	consensus.RegisterConsensusProvider(
		consensusPb.ConsensusType_SOLO,
		func(config *utils.ConsensusImplConfig) (protocol.ConsensusEngine, error) {
			return solo.New(config)
		},
	)

	consensus.RegisterConsensusProvider(
		consensusPb.ConsensusType_DPOS,
		func(config *utils.ConsensusImplConfig) (protocol.ConsensusEngine, error) {
			tbftEngine, err := tbft.New(config) // DPoS based in TBFT
			if err != nil {
				return nil, err
			}
			dposEngine := dpos.NewDPoSImpl(config, tbftEngine)
			return dposEngine, nil
		},
	)

	consensus.RegisterConsensusProvider(
		consensusPb.ConsensusType_RAFT,
		func(config *utils.ConsensusImplConfig) (protocol.ConsensusEngine, error) {
			return raft.New(config)
		},
	)

	consensus.RegisterConsensusProvider(
		consensusPb.ConsensusType_TBFT,
		func(config *utils.ConsensusImplConfig) (protocol.ConsensusEngine, error) {
			return tbft.New(config)
		},
	)

	consensus.RegisterConsensusProvider(
		consensusPb.ConsensusType_MAXBFT,
		func(config *utils.ConsensusImplConfig) (protocol.ConsensusEngine, error) {
			return maxbft.New(config)
		},
	)
}
