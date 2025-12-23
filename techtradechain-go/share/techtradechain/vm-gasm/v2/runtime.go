/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gasm

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"sync"

	"techtradechain.com/techtradechain/common/v2/serialize"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-gasm/v2/gasm-go/hostfunc"
	"techtradechain.com/techtradechain/vm-gasm/v2/gasm-go/waci"
	"techtradechain.com/techtradechain/vm-gasm/v2/gasm-go/wasi"
	"techtradechain.com/techtradechain/vm-gasm/v2/gasm-go/wasm"
	"github.com/golang/groupcache/lru"
)

const (
	LruCacheSize = 64
)

type wasmModMap struct {
	modCache *lru.Cache
}

var inst *wasmModMap
var mu sync.Mutex

func putContractDecodedMod(chainId string, contractId *commonPb.Contract, mod *wasm.Module) {
	mu.Lock()
	defer mu.Unlock()

	if inst == nil {
		inst = &wasmModMap{
			modCache: lru.New(LruCacheSize),
		}
	}
	modName := chainId + contractId.Name + protocol.ContractStoreSeparator + contractId.Version
	inst.modCache.Add(modName, mod)
}

func getContractDecodedMod(chainId string, contractId *commonPb.Contract) *wasm.Module {
	mu.Lock()
	defer mu.Unlock()

	if inst == nil {
		inst = &wasmModMap{
			modCache: lru.New(LruCacheSize),
		}
	}

	modName := chainId + contractId.Name + protocol.ContractStoreSeparator + contractId.Version
	if mod, ok := inst.modCache.Get(modName); ok {
		return mod.(*wasm.Module)
	}
	return nil
}

func removeContractDecodedMod(chainId string, contractId *commonPb.Contract) *wasm.Module {
	mu.Lock()
	defer mu.Unlock()

	if inst == nil {
		inst = &wasmModMap{
			modCache: lru.New(LruCacheSize),
		}
	}

	modName := chainId + contractId.Name + protocol.ContractStoreSeparator + contractId.Version
	inst.modCache.Remove(modName)
	return nil
}

type InstancesManager struct {
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

func (*InstancesManager) NewRuntimeInstance(txSimContext protocol.TxSimContext, chainId, method, codePath string,
	contract *commonPb.Contract, byteCode []byte, log protocol.Logger) (protocol.RuntimeInstance, error) {
	return &RuntimeInstance{
		ChainId: chainId,
		Log:     log,
	}, nil
}

// RuntimeInstance gasm runtime
type RuntimeInstance struct {
	ChainId string
	Log     protocol.Logger
}

// Invoke contract by call vm, implement protocol.RuntimeInstance
func (r *RuntimeInstance) Invoke(contractId *commonPb.Contract, method string, byteCode []byte, //nolint: gocyclo
	parameters map[string][]byte, txContext protocol.TxSimContext, gasUsed uint64) (
	contractResult *commonPb.ContractResult, specialTxType protocol.ExecOrderTxType) {
	tx := txContext.GetTx()

	defer func() {
		if err := recover(); err != nil {
			r.Log.Errorf("failed to invoke gasm, tx id:%s, error:%s", tx.Payload.TxId, err)
			// if panic, set return value
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

	// set default return value
	contractResult = &commonPb.ContractResult{
		Code:    uint32(0),
		Result:  nil,
		Message: "",
	}
	specialTxType = protocol.ExecOrderTxTypeNormal

	var vm *wasm.VirtualMachine
	var mod *wasm.Module
	var err error
	waciInstance := &waci.WaciInstance{
		TxSimContext:   txContext,
		ContractId:     contractId,
		ContractResult: contractResult,
		Log:            r.Log,
		ChainId:        r.ChainId,
		Method:         method,
		SpecialTxType:  protocol.ExecOrderTxTypeNormal,
	}
	wasiInstance := &wasi.WasiInstance{}
	builder := newBuilder(wasiInstance, waciInstance)
	externalMods := builder.Done()

	baseMod := getContractDecodedMod(r.ChainId, contractId)
	if baseMod == nil {
		if baseMod, err = wasm.DecodeModule(bytes.NewBuffer(byteCode)); err != nil {
			contractResult.Code = 1
			contractResult.Message = err.Error()
			r.Log.Errorf("invoke gasm, tx id:%s, error= %s, bytecode len=%d",
				tx.Payload.TxId, err.Error(), len(byteCode))
			return
		}

		if err = baseMod.BuildIndexSpaces(externalMods); err != nil {
			contractResult.Code = 1
			contractResult.Message = err.Error()
			r.Log.Errorf("invoke gasm, failed to build wasm index space, tx id:%s, error= %s, bytecode len=%d",
				tx.Payload.TxId, err.Error(), len(byteCode))
			return
		}
		putContractDecodedMod(r.ChainId, contractId, baseMod)
		mod = baseMod
	} else {
		mod = &wasm.Module{
			SecTypes:     baseMod.SecTypes,
			SecImports:   baseMod.SecImports,
			SecFunctions: baseMod.SecFunctions,
			SecTables:    baseMod.SecTables,
			SecMemory:    baseMod.SecMemory,
			SecGlobals:   baseMod.SecGlobals,
			SecExports:   baseMod.SecExports,
			SecStart:     baseMod.SecStart,
			SecElements:  baseMod.SecElements,
			SecCodes:     baseMod.SecCodes,
			SecData:      baseMod.SecData,
		}
		if err = mod.BuildIndexSpacesUsingOldNativeFunction(externalMods, baseMod.IndexSpace.Function); err != nil {
			contractResult.Code = 1
			contractResult.Message = err.Error()
			r.Log.Errorf("invoke gasm, failed to build wasm index space using old native function, tx id:%s, "+
				"error= %s, bytecode len=%d", tx.Payload.TxId, err.Error(), len(byteCode))
			return
		}
	}

	if vm, err = wasm.NewVM(mod, gasUsed, protocol.GasLimit, protocol.TimeLimit); err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		r.Log.Errorf("invoke gasm,tx id:%s, error= %s", tx.Payload.TxId, err.Error())
		r.removeModByMethod(method, contractId, txContext)
		return
	}
	var paramMarshalBytes []byte
	var runtimeSdkType []uint64
	if runtimeSdkType, _, err = vm.ExecExportedFunction(protocol.ContractRuntimeTypeMethod); err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		r.Log.Errorf("invoke gasm,tx id:%s, failed to call args(), error=", tx.Payload.TxId, err.Error())
		r.removeModByMethod(method, contractId, txContext)
		return
	}

	parameters[protocol.ContractContextPtrParam] = []byte("0") // 兼容rust
	if uint64(commonPb.RuntimeType_GASM) == runtimeSdkType[0] {
		ec := serialize.NewEasyCodecWithMap(parameters)
		paramMarshalBytes = ec.Marshal()
	} else {
		r.runtimeTypeError(contractId, runtimeSdkType, txContext, contractResult)
		return
	}

	var allocateSize = uint64(len(paramMarshalBytes))
	var allocatePtr []uint64
	if allocatePtr, _, err = vm.ExecExportedFunction(protocol.ContractAllocateMethod, allocateSize); err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		r.Log.Errorf("invoke gasm, tx id:%s,failed to allocate, error=", tx.GetPayload().TxId, err.Error())
		r.removeModByMethod(method, contractId, txContext)
		return
	}

	copy(vm.Memory[allocatePtr[0]:allocatePtr[0]+allocateSize], paramMarshalBytes)

	// run invoke method may modify waciInstance's ExecOrderTxType
	if ret, retTypes, err := vm.ExecExportedFunction(method); err != nil {
		contractResult.Code = 1
		contractResult.Message = err.Error()
		r.Log.Errorf("invoke gasm, tx id:%s,error=%+v", tx.GetPayload().TxId, err.Error())
		r.removeModByMethod(method, contractId, txContext)
	} else {
		contractResult.ContractEvent = waciInstance.ContractEvent
		r.Log.Debugf("invoke gasm success, tx id:%s, gas cost %+v,[IGNORE: ret %+v, retTypes %+v]",
			tx.GetPayload().TxId, vm.Gas, ret, retTypes)
	}
	specialTxType = waciInstance.SpecialTxType

	contractResult.GasUsed = vm.Gas
	return
}

func (r *RuntimeInstance) runtimeTypeError(contractId *commonPb.Contract, runtimeSdkType []uint64,
	txContext protocol.TxSimContext, contractResult *commonPb.ContractResult) {
	msg := fmt.Sprintf("runtime type error, expect gasm:%d, but got %d", uint64(commonPb.RuntimeType_GASM),
		runtimeSdkType[0])
	contractResult.Code = 1
	contractResult.Message = msg
	r.Log.Errorf(msg)
	if txContext.GetBlockVersion() >= 2201 {
		removeContractDecodedMod(r.ChainId, contractId)
	}
}

// removeModByMethod
func (r *RuntimeInstance) removeModByMethod(method string, contractId *commonPb.Contract,
	txContext protocol.TxSimContext) {
	if method == syscontract.ContractManageFunction_INIT_CONTRACT.String() &&
		txContext.GetBlockVersion() >= 2201 {
		removeContractDecodedMod(r.ChainId, contractId)
	}
}

func newBuilder(wasiInstance *wasi.WasiInstance, waciInstance *waci.WaciInstance) *hostfunc.ModuleBuilder {
	builder := hostfunc.NewModuleBuilder()
	builder.MustSetFunction(wasi.WasiUnstableModuleName, "fd_write", wasiInstance.FdWrite)
	builder.MustSetFunction(wasi.WasiModuleName, "fd_write", wasiInstance.FdWrite)
	builder.MustSetFunction(wasi.WasiModuleName, "fd_read", wasiInstance.FdRead)
	builder.MustSetFunction(wasi.WasiModuleName, "fd_close", wasiInstance.FdClose)
	builder.MustSetFunction(wasi.WasiModuleName, "fd_seek", wasiInstance.FdSeek)
	builder.MustSetFunction(wasi.WasiModuleName, "proc_exit", wasiInstance.ProcExit)

	builder.MustSetFunction(waci.WaciModuleName, "sys_call", waciInstance.SysCall)
	builder.MustSetFunction(waci.WaciModuleName, "log_message", waciInstance.LogMsg)

	//builder.MustSetFunction(waci.WaciModuleName, "get_state_len_from_chain", waciInstance.GetStateLen)
	//builder.MustSetFunction(waci.WaciModuleName, "get_state_from_chain", waciInstance.GetState)
	//builder.MustSetFunction(waci.WaciModuleName, "put_state_to_chain", waciInstance.PutState)
	//builder.MustSetFunction(waci.WaciModuleName, "delete_state_from_chain", waciInstance.DeleteState)
	//builder.MustSetFunction(waci.WaciModuleName, "success_result_to_chain", waciInstance.SuccessResult)
	//builder.MustSetFunction(waci.WaciModuleName, "error_result_to_chain", waciInstance.ErrorResult)
	//builder.MustSetFunction(waci.WaciModuleName, "call_contract_len_from_chain", waciInstance.CallContractLen)
	//builder.MustSetFunction(waci.WaciModuleName, "call_contract_from_chain", waciInstance.CallContract)
	return builder
}
