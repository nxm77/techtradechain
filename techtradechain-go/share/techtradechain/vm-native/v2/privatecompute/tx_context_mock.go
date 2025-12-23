/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package privatecompute

import (
	"sync"

	"techtradechain.com/techtradechain/pb-go/v2/vm"

	acPb "techtradechain.com/techtradechain/pb-go/v2/accesscontrol"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/protocol/v2"
)

type dataStore map[string][]byte

// TxContextMock mock tx context
type TxContextMock struct {
	lock     *sync.Mutex
	cacheMap dataStore
}

// GetSnapshot mock
func (s *TxContextMock) GetSnapshot() protocol.Snapshot {
	//TODO implement me
	panic("implement me")
}

// GetBlockFingerprint mock
func (s *TxContextMock) GetBlockFingerprint() string {
	panic("implement me")
}

// GetStrAddrFromPbMember mock
func (s *TxContextMock) GetStrAddrFromPbMember(pbMember *acPb.Member) (string, error) {
	//TODO implement me
	panic("implement me")
}

// GetKeys returns batch key values
func (s *TxContextMock) GetKeys(keys []*vm.BatchKey) ([]*vm.BatchKey, error) {
	//TODO implement me
	panic("implement me")
}

// GetNoRecord returns no record
func (s *TxContextMock) GetNoRecord(contractName string, key []byte) ([]byte, error) {
	panic("implement me")
}

// GetBlockTimestamp returns block timestamp
func (s *TxContextMock) GetBlockTimestamp() int64 {
	panic("implement me")
}

// GetIterHandle returns iter
func (s *TxContextMock) GetIterHandle(index int32) (interface{}, bool) {
	panic("implement me")
}

// SetIterHandle set iter
func (s *TxContextMock) SetIterHandle(index int32, iter interface{}) {
	panic("implement me")
}

// GetHistoryIterForKey returns history iter for key
func (s *TxContextMock) GetHistoryIterForKey(contractName string, key []byte) (protocol.KeyHistoryIterator, error) {
	panic("implement me")
}

// PutIntoReadSet put into read set
func (mock *TxContextMock) PutIntoReadSet(contractName string, key []byte, value []byte) {
	panic("implement me")
}

// GetContractByName returns contract by name
func (mock *TxContextMock) GetContractByName(name string) (*commonPb.Contract, error) {
	panic("implement me")
}

// GetContractBytecode returns contract by code
func (mock *TxContextMock) GetContractBytecode(name string) ([]byte, error) {
	panic("implement me")
}

func newTxContextMock(cache dataStore) *TxContextMock {
	return &TxContextMock{
		lock:     &sync.Mutex{},
		cacheMap: cache,
	}
}

// GetBlockVersion returns block version
func (mock *TxContextMock) GetBlockVersion() uint32 {
	return protocol.DefaultBlockVersion
}

// Get key from cache, record this operation to read set
func (mock *TxContextMock) Get(name string, key []byte) ([]byte, error) {
	mock.lock.Lock()
	defer mock.lock.Unlock()

	k := string(key)
	if name != "" {
		k = name + "::" + k
	}

	return mock.cacheMap[k], nil
}

// Put key into cache
func (mock *TxContextMock) Put(name string, key []byte, value []byte) error {
	mock.lock.Lock()
	defer mock.lock.Unlock()

	k := string(key)
	if name != "" {
		k = name + "::" + k
	}

	mock.cacheMap[k] = value
	return nil
}

// Del Delete key from cache
func (mock *TxContextMock) Del(name string, key []byte) error {
	mock.lock.Lock()
	defer mock.lock.Unlock()

	k := string(key)
	if name != "" {
		k = name + "::" + k
	}

	mock.cacheMap[k] = nil
	return nil
}

// CallContract Cross contract call, return (contract result, gas used)
func (*TxContextMock) CallContract(caller, contract *commonPb.Contract,
	method string,
	byteCode []byte,
	parameter map[string][]byte,
	gasUsed uint64,
	refTxType commonPb.TxType,
) (*commonPb.ContractResult, protocol.ExecOrderTxType, commonPb.TxStatusCode) {

	panic("implement me")
}

// GetCurrentResult Get cross contract call result, cache for len
func (*TxContextMock) GetCurrentResult() []byte {
	panic("implement me")
}

// GetTx get related transaction
func (*TxContextMock) GetTx() *commonPb.Transaction {
	panic("implement me")
}

// GetBlockHeight returns current block height
func (mock *TxContextMock) GetBlockHeight() uint64 {
	return 0
}

// GetTxResult returns the tx result
func (mock *TxContextMock) GetTxResult() *commonPb.Result {
	panic("implement me")
}

// SetTxResult set tx result
func (mock *TxContextMock) SetTxResult(txResult *commonPb.Result) {
	panic("implement me")
}

// GetTxRWSet returns tx rwset
func (mock *TxContextMock) GetTxRWSet(runVmSuccess bool) *commonPb.TxRWSet {
	panic("implement me")
}

// GetCreator returns creator
func (mock *TxContextMock) GetCreator(namespace string) *acPb.Member {
	panic("implement me")
}

// GetSender returns sender
func (mock *TxContextMock) GetSender() *acPb.Member {
	panic("implement me")
}

// GetBlockchainStore returns blockchain store
func (mock *TxContextMock) GetBlockchainStore() protocol.BlockchainStore {
	panic("implement me")
}

// GetLastChainConfig returns last chain config
func (mock *TxContextMock) GetLastChainConfig() *config.ChainConfig {
	panic("implement me")
}

// GetAccessControl returns access control
func (mock *TxContextMock) GetAccessControl() (protocol.AccessControlProvider, error) {
	return &ACProviderMock{}, nil
}

// GetChainNodesInfoProvider returns organization service
func (mock *TxContextMock) GetChainNodesInfoProvider() (protocol.ChainNodesInfoProvider, error) {
	panic("implement me")
}

// GetTxExecSeq returns tx exec seq
func (mock *TxContextMock) GetTxExecSeq() int {
	panic("implement me")
}

// SetTxExecSeq set tx exec seq
func (mock *TxContextMock) SetTxExecSeq(i int) {
	panic("implement me")
}

// GetDepth get cross contract call depth
func (mock *TxContextMock) GetDepth() int {
	panic("implement me")
}

// GetBlockProposer returns current block proposer
func (mock *TxContextMock) GetBlockProposer() *acPb.Member {
	panic("implement me")
}

// PutRecord put sql state into cache
func (mock *TxContextMock) PutRecord(contractName string, value []byte, sqlType protocol.SqlType) {
	panic("implement me")
}

// Select range query for key [start, limit)
func (mock *TxContextMock) Select(name string, startKey []byte, limit []byte) (protocol.StateIterator, error) {
	panic("implement me")
}

// GetTxRWMapByContractName get the read-write map of the specified contract of the current transaction
func (mock *TxContextMock) GetTxRWMapByContractName(contractName string) (
	map[string]*commonPb.TxRead, map[string]*commonPb.TxWrite) {
	//TODO implement me
	panic("implement me")
}

// GetCrossInfo get contract call link information
func (mock *TxContextMock) GetCrossInfo() uint64 {
	//TODO implement me
	panic("implement me")
}

// HasUsed judge whether the specified common.RuntimeType has appeared in the previous depth
// in the current cross-link
func (mock *TxContextMock) HasUsed(runtimeType commonPb.RuntimeType) bool {
	//TODO implement me
	panic("implement me")
}

// RecordRuntimeTypeIntoCrossInfo record the new contract call information to the top of crossInfo
func (mock *TxContextMock) RecordRuntimeTypeIntoCrossInfo(runtimeType commonPb.RuntimeType) {
	//TODO implement me
	panic("implement me")
}

// RemoveRuntimeTypeFromCrossInfo remove the top-level information from the crossInfo
func (mock *TxContextMock) RemoveRuntimeTypeFromCrossInfo() {
	//TODO implement me
	panic("implement me")
}

// GetGasRemaining mocks base method.
func (mock *TxContextMock) GetGasRemaining() uint64 {
	panic("implement me")
}

// SubtractGas mocks base method.
func (mock *TxContextMock) SubtractGas(gasUsed uint64) error {
	//TODO implement me
	panic("implement me")
}
