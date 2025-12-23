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

package opcodes

type OpCode byte

const (
	MaxOpCodesCount = 256
)

const (
	STOP OpCode = iota + 0x00
	ADD
	MUL
	SUB
	DIV
	SDIV
	MOD
	SMOD
	ADDMOD
	MULMOD
	EXP
	SIGNEXTEND
)

const (
	unusedX0C OpCode = iota + 0x0C
	unusedX0D
	unusedX0E
	unusedX0F
)

const (
	LT OpCode = iota + 0x10
	GT
	SLT
	SGT
	EQ
	ISZERO
)

const (
	AND OpCode = iota + 0x16
	OR
	XOR
	NOT
	BYTE
	SHL
	SHR
	SAR
)

const (
	unusedX1E OpCode = iota + 0x1E
	unusedX1F
)

const (
	SHA3 OpCode = iota + 0x20
)

const (
	unusedX21 OpCode = iota + 0x21
	unusedX22
	unusedX23
	unusedX24
	unusedX25
	unusedX26
	unusedX27
	unusedX28
	unusedX29
	unusedX2A
	unusedX2B
	unusedX2C
	unusedX2D
	unusedX2E
	unusedX2F
)

const (
	ADDRESS OpCode = iota + 0x30
	BALANCE
	ORIGIN
	CALLER
	CALLVALUE
	CALLDATALOAD
	CALLDATASIZE
	CALLDATACOPY
	CODESIZE
	CODECOPY
	GASPRICE
	EXTCODESIZE
	EXTCODECOPY
	RETURNDATASIZE
	RETURNDATACOPY
	EXTCODEHASH
	BLOCKHASH
	COINBASE
	TIMESTAMP
	NUMBER
	DIFFICULTY
	GASLIMIT
)

const (
	//unusedX46 OpCode = iota + 0x46
	//unusedX47
	//unusedX48
	CHAINID OpCode = iota + 0x46
	SELFBALANCE
	BASEFEE
	unusedX49
	unusedX4A
	unusedX4B
	unusedX4C
	unusedX4D
	unusedX4E
	unusedX4F
)

const (
	POP OpCode = iota + 0x50
	MLOAD
	MSTORE
	MSTORE8
	SLOAD
	SSTORE
	JUMP
	JUMPI
	PC
	MSIZE
	GAS
	JUMPDEST
)

const (
	unusedX5C OpCode = iota + 0x5C
	unusedX5D
	unusedX5E
	//unusedX5F
	PUSH0
)

const (
	PUSH1 OpCode = iota + 0x60
	PUSH2
	PUSH3
	PUSH4
	PUSH5
	PUSH6
	PUSH7
	PUSH8
	PUSH9
	PUSH10
	PUSH11
	PUSH12
	PUSH13
	PUSH14
	PUSH15
	PUSH16
	PUSH17
	PUSH18
	PUSH19
	PUSH20
	PUSH21
	PUSH22
	PUSH23
	PUSH24
	PUSH25
	PUSH26
	PUSH27
	PUSH28
	PUSH29
	PUSH30
	PUSH31
	PUSH32
)

const (
	DUP1 OpCode = iota + 0x80
	DUP2
	DUP3
	DUP4
	DUP5
	DUP6
	DUP7
	DUP8
	DUP9
	DUP10
	DUP11
	DUP12
	DUP13
	DUP14
	DUP15
	DUP16
)

const (
	SWAP1 OpCode = iota + 0x90
	SWAP2
	SWAP3
	SWAP4
	SWAP5
	SWAP6
	SWAP7
	SWAP8
	SWAP9
	SWAP10
	SWAP11
	SWAP12
	SWAP13
	SWAP14
	SWAP15
	SWAP16
)

const (
	LOG0 OpCode = iota + 0xA0
	LOG1
	LOG2
	LOG3
	LOG4
)

const (
	unusedXA5 OpCode = iota + 0xA5
	unusedXA6
	unusedXA7
	unusedXA8
	unusedXA9
	unusedXAA
	unusedXAB
	unusedXAC
	unusedXAD
	unusedXAE
	unusedXAF
	unusedXB0 //unofficial PUSH
	unusedXB1 //unofficial DUP
	unusedXB2 //unofficial SWAP
	//unusedXB3
	//unusedXB4
	TLOAD
	TSTORE
	unusedXB5
	unusedXB6
	unusedXB7
	unusedXB8
	unusedXB9
	unusedXBA
	unusedXBB
	unusedXBC
	unusedXBD
	unusedXBE
	unusedXBF
	unusedXC0
	unusedXC1
	unusedXC2
	unusedXC3
	unusedXC4
	unusedXC5
	unusedXC6
	unusedXC7
	unusedXC8
	unusedXC9
	unusedXCA
	unusedXCB
	unusedXCC
	unusedXCD
	unusedXCE
	unusedXCF
	unusedXD0
	unusedXD1
	unusedXD2
	unusedXD3
	unusedXD4
	unusedXD5
	unusedXD6
	unusedXD7
	unusedXD8
	unusedXD9
	unusedXDA
	unusedXDB
	unusedXDC
	unusedXDD
	unusedXDE
	unusedXDF
	unusedXE0
	unusedXE1
	unusedXE2
	unusedXE3
	unusedXE4
	unusedXE5
	unusedXE6
	unusedXE7
	unusedXE8
	unusedXE9
	unusedXEA
	unusedXEB
	unusedXEC
	unusedXED
	unusedXEE
	unusedXEF
)

const (
	CREATE OpCode = iota + 0xF0
	CALL
	CALLCODE
	RETURN
	DELEGATECALL
	CREATE2
)

const (
	unusedXF6 OpCode = iota + 0xF6
	unusedXF7
	unusedXF8
	unusedXF9
)

const (
	STATICCALL OpCode = iota + 0xFA
)

const (
	unusedXFB OpCode = iota + 0xFB
	unusedXFC
)

const (
	REVERT OpCode = iota + 0xFD
)

const (
	unusedXFE OpCode = iota + 0xFE
)

const (
	SELFDESTRUCT OpCode = iota + 0xFF
)

var OpCodeMap map[int]string

func init() {
	OpCodeMap = make(map[int]string)
	OpCodeMap[int(STOP)] = "STOP"
	OpCodeMap[int(ADD)] = "ADD"
	OpCodeMap[int(MUL)] = "MUL"
	OpCodeMap[int(SUB)] = "SUB"
	OpCodeMap[int(DIV)] = "DIV"
	OpCodeMap[int(SDIV)] = "SDIV"
	OpCodeMap[int(SMOD)] = "SMOD"
	OpCodeMap[int(ADDMOD)] = "ADDMOD"
	OpCodeMap[int(MULMOD)] = "MULMOD"
	OpCodeMap[int(EXP)] = "EXP"
	OpCodeMap[int(SIGNEXTEND)] = "SIGNEXTEND"

	OpCodeMap[int(LT)] = "LT"
	OpCodeMap[int(GT)] = "GT"
	OpCodeMap[int(SLT)] = "SLT"
	OpCodeMap[int(SGT)] = "SGT"
	OpCodeMap[int(EQ)] = "EQ"
	OpCodeMap[int(ISZERO)] = "ISZERO"
	OpCodeMap[int(AND)] = "AND"
	OpCodeMap[int(OR)] = "OR"
	OpCodeMap[int(XOR)] = "XOR"
	OpCodeMap[int(NOT)] = "NOT"
	OpCodeMap[int(BYTE)] = "BYTE"
	OpCodeMap[int(SHL)] = "SHL"
	OpCodeMap[int(SHR)] = "SHR"
	OpCodeMap[int(SAR)] = "SAR"

	OpCodeMap[int(SHA3)] = "SHA3"

	OpCodeMap[int(ADDRESS)] = "ADDRESS"
	OpCodeMap[int(BALANCE)] = "BALANCE"
	OpCodeMap[int(ORIGIN)] = "ORIGIN"
	OpCodeMap[int(CALLER)] = "CALLER"
	OpCodeMap[int(CALLVALUE)] = "CALLVALUE"
	OpCodeMap[int(CALLDATALOAD)] = "CALLDATALOAD"
	OpCodeMap[int(CALLDATASIZE)] = "CALLDATASIZE"
	OpCodeMap[int(CALLDATACOPY)] = "CALLDATACOPY"
	OpCodeMap[int(CODESIZE)] = "CODESIZE"
	OpCodeMap[int(CODECOPY)] = "CODECOPY"
	OpCodeMap[int(GASPRICE)] = "GASPRICE"
	OpCodeMap[int(EXTCODESIZE)] = "EXTCODESIZE"
	OpCodeMap[int(EXTCODECOPY)] = "EXTCODECOPY"
	OpCodeMap[int(RETURNDATASIZE)] = "RETURNDATASIZE"
	OpCodeMap[int(RETURNDATACOPY)] = "RETURNDATACOPY"
	OpCodeMap[int(EXTCODEHASH)] = "EXTCODEHASH"
	OpCodeMap[int(BLOCKHASH)] = "BLOCKHASH"
	OpCodeMap[int(COINBASE)] = "COINBASE"
	OpCodeMap[int(TIMESTAMP)] = "TIMESTAMP"
	OpCodeMap[int(NUMBER)] = "NUMBER"
	OpCodeMap[int(DIFFICULTY)] = "DIFFICULTY"
	OpCodeMap[int(GASLIMIT)] = "GASLIMIT"

	OpCodeMap[int(POP)] = "POP"
	OpCodeMap[int(MLOAD)] = "MLOAD"
	OpCodeMap[int(MSTORE)] = "MSTORE"
	OpCodeMap[int(MSTORE8)] = "MSTORE8"
	OpCodeMap[int(SLOAD)] = "SLOAD"
	OpCodeMap[int(SSTORE)] = "SSTORE"
	OpCodeMap[int(JUMP)] = "JUMP"
	OpCodeMap[int(JUMPI)] = "JUMPI"
	OpCodeMap[int(PC)] = "PC"
	OpCodeMap[int(MSIZE)] = "MSIZE"
	OpCodeMap[int(GAS)] = "GAS"
	OpCodeMap[int(JUMPDEST)] = "JUMPDEST"

	OpCodeMap[int(PUSH1)] = "PUSH1"
	OpCodeMap[int(PUSH2)] = "PUSH2"
	OpCodeMap[int(PUSH3)] = "PUSH3"
	OpCodeMap[int(PUSH4)] = "PUSH4"
	OpCodeMap[int(PUSH5)] = "PUSH5"
	OpCodeMap[int(PUSH6)] = "PUSH6"
	OpCodeMap[int(PUSH7)] = "PUSH7"
	OpCodeMap[int(PUSH8)] = "PUSH8"
	OpCodeMap[int(PUSH9)] = "PUSH9"
	OpCodeMap[int(PUSH10)] = "PUSH10"
	OpCodeMap[int(PUSH11)] = "PUSH11"
	OpCodeMap[int(PUSH12)] = "PUSH12"
	OpCodeMap[int(PUSH13)] = "PUSH13"
	OpCodeMap[int(PUSH14)] = "PUSH14"
	OpCodeMap[int(PUSH15)] = "PUSH15"
	OpCodeMap[int(PUSH16)] = "PUSH16"
	OpCodeMap[int(PUSH17)] = "PUSH17"
	OpCodeMap[int(PUSH18)] = "PUSH18"
	OpCodeMap[int(PUSH19)] = "PUSH19"
	OpCodeMap[int(PUSH20)] = "PUSH20"
	OpCodeMap[int(PUSH21)] = "PUSH21"
	OpCodeMap[int(PUSH22)] = "PUSH22"
	OpCodeMap[int(PUSH23)] = "PUSH23"
	OpCodeMap[int(PUSH24)] = "PUSH24"
	OpCodeMap[int(PUSH25)] = "PUSH24"
	OpCodeMap[int(PUSH26)] = "PUSH26"
	OpCodeMap[int(PUSH27)] = "PUSH27"
	OpCodeMap[int(PUSH28)] = "PUSH28"
	OpCodeMap[int(PUSH29)] = "PUSH29"
	OpCodeMap[int(PUSH30)] = "PUSH30"
	OpCodeMap[int(PUSH31)] = "PUSH31"
	OpCodeMap[int(PUSH32)] = "PUSH32"

	OpCodeMap[int(DUP1)] = "DUP1"
	OpCodeMap[int(DUP2)] = "DUP2"
	OpCodeMap[int(DUP3)] = "DUP3"
	OpCodeMap[int(DUP4)] = "DUP4"
	OpCodeMap[int(DUP5)] = "DUP5"
	OpCodeMap[int(DUP6)] = "DUP6"
	OpCodeMap[int(DUP7)] = "DUP7"
	OpCodeMap[int(DUP8)] = "DUP8"
	OpCodeMap[int(DUP9)] = "DUP9"
	OpCodeMap[int(DUP10)] = "DUP10"
	OpCodeMap[int(DUP11)] = "DUP11"
	OpCodeMap[int(DUP12)] = "DUP12"
	OpCodeMap[int(DUP13)] = "DUP13"
	OpCodeMap[int(DUP14)] = "DUP14"
	OpCodeMap[int(DUP15)] = "DUP15"
	OpCodeMap[int(DUP16)] = "DUP16"

	OpCodeMap[int(SWAP1)] = "SWAP1"
	OpCodeMap[int(SWAP2)] = "SWAP2"
	OpCodeMap[int(SWAP3)] = "SWAP3"
	OpCodeMap[int(SWAP4)] = "SWAP4"
	OpCodeMap[int(SWAP5)] = "SWAP5"
	OpCodeMap[int(SWAP6)] = "SWAP6"
	OpCodeMap[int(SWAP7)] = "SWAP7"
	OpCodeMap[int(SWAP8)] = "SWAP8"
	OpCodeMap[int(SWAP9)] = "SWAP9"
	OpCodeMap[int(SWAP10)] = "SWAP10"
	OpCodeMap[int(SWAP11)] = "SWAP11"
	OpCodeMap[int(SWAP12)] = "SWAP12"
	OpCodeMap[int(SWAP13)] = "SWAP13"
	OpCodeMap[int(SWAP14)] = "SWAP14"
	OpCodeMap[int(SWAP15)] = "SWAP15"
	OpCodeMap[int(SWAP16)] = "SWAP16"
	OpCodeMap[int(LOG0)] = "LOG0"
	OpCodeMap[int(LOG1)] = "LOG1"
	OpCodeMap[int(LOG2)] = "LOG2"
	OpCodeMap[int(LOG3)] = "LOG3"
	OpCodeMap[int(LOG4)] = "LOG4"

	OpCodeMap[int(CREATE)] = "CREATE"
	OpCodeMap[int(CALL)] = "CALL"
	OpCodeMap[int(CALLCODE)] = "CALLCODE"
	OpCodeMap[int(RETURN)] = "RETURN"
	OpCodeMap[int(DELEGATECALL)] = "DELEGATECALL"
	OpCodeMap[int(CREATE2)] = "CREATE2"
	OpCodeMap[int(STATICCALL)] = "STATICCALL"
	OpCodeMap[int(REVERT)] = "REVERT"
	OpCodeMap[int(SELFDESTRUCT)] = "LOG4"
}
