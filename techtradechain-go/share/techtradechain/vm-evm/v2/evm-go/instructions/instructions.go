/*
 * Copyright 2020 The SealEVM Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package instructions

import (
	"fmt"
	"reflect"
	"runtime"
	"time"

	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/params"

	"techtradechain.com/techtradechain/common/v2/evmutils"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/environment"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/memory"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/opcodes"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/stack"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/storage"
	"techtradechain.com/techtradechain/vm-evm/v2/evm-go/utils"
)

type DynamicGasCostSetting struct {
	EXPBytesCost   uint64
	SHA3ByteCost   uint64
	MemoryByteCost uint64
	LogByteCost    uint64
}

type GasSetting struct {
	ActionConstCost [opcodes.MaxOpCodesCount]uint64
	NewAccountCost  uint64
	DynamicCost     DynamicGasCostSetting
}

const dumpFlag = false

func DefaultGasSetting() *GasSetting {
	gs := &GasSetting{}

	for i := range gs.ActionConstCost {
		gs.ActionConstCost[i] = 3
	}

	gs.ActionConstCost[opcodes.EXP] = 10
	gs.ActionConstCost[opcodes.SHA3] = 30
	gs.ActionConstCost[opcodes.LOG0] = 375
	gs.ActionConstCost[opcodes.LOG1] = 375 * 2
	gs.ActionConstCost[opcodes.LOG2] = 375 * 3
	gs.ActionConstCost[opcodes.LOG3] = 375 * 4
	gs.ActionConstCost[opcodes.LOG4] = 375 * 5
	gs.ActionConstCost[opcodes.SLOAD] = 800
	gs.ActionConstCost[opcodes.SSTORE] = 5000
	gs.ActionConstCost[opcodes.SELFDESTRUCT] = 30000

	gs.ActionConstCost[opcodes.CREATE] = 32000
	gs.ActionConstCost[opcodes.CREATE2] = 32000

	gs.DynamicCost.EXPBytesCost = 50
	gs.DynamicCost.SHA3ByteCost = 6
	gs.DynamicCost.MemoryByteCost = 2
	gs.DynamicCost.LogByteCost = 8

	return gs
}

type ConstOpGasCostSetting [opcodes.MaxOpCodesCount]uint64

type instructionsContext struct {
	stack       *stack.Stack
	memory      *memory.Memory
	storage     *storage.Storage //same pointer to evm.storage.ResultCache
	environment *environment.Context

	vm interface{}

	pc           uint64
	pcCount      uint64
	timeUsed     int64
	readOnly     bool
	gasSetting   *GasSetting
	lastReturn   []byte
	gasRemaining *evmutils.Int
	closureExec  ClosureExecute
	exitOpCode   opcodes.OpCode
}

type opCodeAction func(ctx *instructionsContext) ([]byte, error)
type opCodeInstruction struct {
	action            opCodeAction
	requireStackDepth int
	willIncreaseStack int

	//flags
	enabled  bool
	jumps    bool
	isWriter bool
	returns  bool
	finished bool
}

type IInstructions interface {
	ExecuteContract(isCreate bool) ([]byte, uint64, []byte, []byte, error)
	GetPcCountAndTimeUsed() (uint64, int64)
	SetGasLimit(uint64)
	GetGasLeft() uint64
	SetReadOnly()
	IsReadOnly() bool
	ExitOpCode() opcodes.OpCode
}

var instructionTable [opcodes.MaxOpCodesCount]opCodeInstruction

// returns offset, size in type uint64
func (i *instructionsContext) memoryGasCostAndMalloc(offset *evmutils.Int, size *evmutils.Int) (uint64, uint64, uint64, error) {
	gasLeft := i.gasRemaining.Uint64()
	o, s, increased, err := i.memory.WillIncrease(offset, size)
	if err != nil {
		return o, s, gasLeft, err
	}

	gasCost := increased * i.gasSetting.DynamicCost.MemoryByteCost
	if gasLeft < gasCost {
		return o, s, gasLeft, utils.ErrOutOfGas
	}

	gasLeft -= gasCost
	i.gasRemaining.SetUint64(gasLeft)

	i.memory.Malloc(o, s)
	return o, s, gasLeft, err
}

func (i *instructionsContext) GetPcCountAndTimeUsed() (uint64, int64) {
	return i.pcCount, i.timeUsed
}

func (i *instructionsContext) SetGasLimit(gasLimit uint64) {
	i.gasRemaining.SetUint64(gasLimit)
}

func (i *instructionsContext) SetReadOnly() {
	i.readOnly = true
}

func (i *instructionsContext) IsReadOnly() bool {
	return i.readOnly
}

func (i *instructionsContext) GetGasLeft() uint64 {
	return i.gasRemaining.Uint64()
}

func (i *instructionsContext) ExitOpCode() opcodes.OpCode {
	return i.exitOpCode
}

// ExecuteContract return execresult, gas, byteCodeHead, byteCodeBody, err
func (i *instructionsContext) ExecuteContract(isCreate bool) ([]byte, uint64, []byte, []byte, error) {
	i.timeUsed = 0
	i.pcCount = 0
	i.pc = 0
	contract := i.environment.Contract

	if len(contract.Code) == 0 {
		return nil, i.gasRemaining.Uint64(), nil, nil, fmt.Errorf("contract code is null")
	}

	var ret []byte
	var err error = nil
	var byteCodeHead []byte
	var byteCodeBody []byte
	startTime := time.Now()
	version := i.storage.GetCurrentBlockVersion()
	for {
		i.pcCount++
		opCode := contract.Code[i.pc]

		instruction := instructionTable[opCode]
		if !instruction.enabled {
			return nil, i.gasRemaining.Uint64(), nil, nil, utils.InvalidOpCode(opCode)
		}

		if instruction.isWriter && i.readOnly {
			return nil, i.gasRemaining.Uint64(), nil, nil, utils.ErrWriteProtection
		}

		err = i.stack.CheckStackDepth(instruction.requireStackDepth, instruction.willIncreaseStack)
		if err != nil {
			break
		}

		gasLeft := i.gasRemaining.Uint64()
		// don't remove, just for debug
		//opCodeStr, exist := opcodes.OpCodeMap[int(opCode)]
		//if !exist {
		//	opCodeStr = fmt.Sprintf("unusedX%X", opCode)
		//}
		//fmt.Printf("opcode: %X:%s; ", opCode, opCodeStr)

		constCost := i.gasSetting.ActionConstCost[opCode]
		if gasLeft >= constCost {
			gasLeft -= constCost
			i.gasRemaining.SetUint64(gasLeft)
		} else {
			err = utils.ErrOutOfGas
			break
		}

		//sysmtemLog.Infof("cnt[%v], pc[%v], opcode[%s], stack%s",
		//	i.pcCount, i.pc, opcodes.OpCode(opCode).String(), i.stack.Print2Str())
		ret, err = instruction.action(i)

		if dumpFlag {
			i.dumpVM(instruction, opCode)
		}
		if instruction.returns {
			i.lastReturn = ret
		}

		if err != nil {
			break
		}

		if !instruction.jumps {
			i.pc += 1
		}

		if instruction.finished {
			i.exitOpCode = opcodes.OpCode(opCode)
			// save byteCode
			if isCreate {
				split := int(i.pc) + 1
				if i.storage.GetCurrentBlockVersion() >= params.V2030300 {
					if split > len(contract.Code) {
						split = len(contract.Code)
					}
				}
				byteCodeHead = contract.Code[0:split]
				byteCodeBody = contract.Code[split:]
			}

			if params.V2218 <= version && version < params.V2300 || version > params.V2030100 {
				if i.exitOpCode == opcodes.REVERT {
					err = utils.ErrExecutionReverted
				}
			}
			break
		}
	}
	i.timeUsed = time.Since(startTime).Microseconds()
	return ret, i.gasRemaining.Uint64(), byteCodeHead, byteCodeBody, err
}

func (i *instructionsContext) dumpVM(instruction opCodeInstruction, opCode byte) {

	name := runtime.FuncForPC(reflect.ValueOf(instruction.action).Pointer()).Name()
	fmt.Println("instruction", i.pc, "\t", name, opCode)

	stackData := i.stack.GetData()
	for index := range stackData {
		pos := len(stackData) - index - 1
		fmt.Printf("stack index [%v] -> [%x]\n", pos, stackData[pos])
	}

	i.memory.All()
	//for n, value := range i.storage.ResultCache.WriteCache {
	//	for k, v := range value {
	//		fmt.Println("current result.Catch.WriteCache", n, "k", k, "v", v.String())
	//	}
	//}
}

func Load() {
	loadStack()
	loadMemory()
	loadStorage()
	loadArithmetic()
	loadBitOperations()
	loadComparision()
	loadEnvironment()
	loadLog()
	loadMisc()
	loadClosure()
	loadPC()
}

func GetInstructionsTable() [opcodes.MaxOpCodesCount]opCodeInstruction {
	return instructionTable
}

func New(
	vm interface{},
	stack *stack.Stack,
	memory *memory.Memory,
	storage *storage.Storage,
	context *environment.Context,
	gasSetting *GasSetting,
	closureExecute ClosureExecute) IInstructions {

	is := &instructionsContext{
		vm:          vm,
		stack:       stack,
		memory:      memory,
		storage:     storage,
		environment: context,
		closureExec: closureExecute,
	}

	is.gasRemaining = evmutils.FromBigInt(context.Transaction.GasLimit.Int)

	if gasSetting != nil {
		is.gasSetting = gasSetting
	} else {
		is.gasSetting = DefaultGasSetting()
	}

	return is
}
