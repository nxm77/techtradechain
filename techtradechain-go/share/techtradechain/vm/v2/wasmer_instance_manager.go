package vm

import (
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"
)

// WasmerInstancesManager interface add CloseRuntimeInstance based VmInstancesManager for `wasmer` module
type WasmerInstancesManager interface {
	NewRuntimeInstance(txSimContext protocol.TxSimContext, chainId, method, codePath string, contract *common.Contract,
		byteCode []byte, log protocol.Logger) (protocol.RuntimeInstance, error)

	// addded for wasmer
	CloseRuntimeInstance(contractName string, contractVersion string) error

	// StartVM Start vm
	StartVM() error
	// StopVM Stop vm
	StopVM() error
}
