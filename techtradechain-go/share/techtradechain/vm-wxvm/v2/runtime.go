/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package wxvm

import (
	"path/filepath"
	"runtime/debug"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-wxvm/v2/xvm"
)

type RuntimeInstance struct {
	ChainId     string
	CodeManager *xvm.CodeManager
	CtxService  *xvm.ContextService
	Log         protocol.Logger
}

type InstancesManager struct {
}

func (*InstancesManager) NewRuntimeInstance(txSimContext protocol.TxSimContext, chainId, method, codePath string,
	contract *commonPb.Contract, byteCode []byte, log protocol.Logger) (protocol.RuntimeInstance, error) {
	fullCodePath := filepath.Join(codePath, chainId, "wxvm")
	return &RuntimeInstance{
		chainId,
		xvm.NewCodeManager(chainId, fullCodePath, log),
		xvm.NewContextService(chainId, log),
		log,
	}, nil
}

// Invoke contract by call vm, implement protocol.RuntimeInstance
func (r *RuntimeInstance) Invoke(contract *commonPb.Contract, method string, byteCode []byte,
	parameters map[string][]byte, txContext protocol.TxSimContext, gasUsed uint64) (
	contractResult *commonPb.ContractResult, specialTxType protocol.ExecOrderTxType) {

	tx := txContext.GetTx()

	defer func() {
		if err := recover(); err != nil {
			r.Log.Errorf("invoke wxvm panic, tx id:%s, error:%s", tx.Payload.TxId, err)
			contractResult.Code = 1
			if e, ok := err.(error); ok {
				contractResult.Message = e.Error()
			} else if e, ok := err.(string); ok {
				contractResult.Message = e
			}
			specialTxType = protocol.ExecOrderTxTypeNormal
			debug.PrintStack()
		}
	}()

	contractResult = &commonPb.ContractResult{
		Code:    uint32(0),
		Result:  nil,
		Message: "",
	}
	specialTxType = protocol.ExecOrderTxTypeNormal

	context := r.CtxService.MakeContext(contract, txContext, contractResult, parameters)
	execCode, err := r.CodeManager.GetExecCode(r.ChainId, contract, byteCode, r.CtxService)
	defer r.CtxService.DestroyContext(context)

	if err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		return
	}

	inst, err := xvm.CreateInstance(context.ID, execCode, method, contract, gasUsed, int64(protocol.GasLimit))
	if err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		return
	} else if err = inst.Exec(); err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		return
	} else {
		contractResult.GasUsed = inst.ExecCtx.GasUsed()
		contractResult.ContractEvent = context.ContractEvent
	}

	return contractResult, protocol.ExecOrderTxTypeNormal
}

func (*InstancesManager) StartVM() error {
	return nil
}

func (*InstancesManager) StopVM() error {
	return nil
}

func (*InstancesManager) BeforeSchedule(blockFingerprint string, blockHeight uint64) {
}

func (*InstancesManager) AfterSchedule(blockFingerprint string, blockHeight uint64) {
}
