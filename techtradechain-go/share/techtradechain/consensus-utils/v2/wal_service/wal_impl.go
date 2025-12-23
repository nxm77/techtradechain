/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package wal_service

import (
	"fmt"
	"sync"

	"techtradechain.com/techtradechain/common/v2/wal"

	"github.com/gogo/protobuf/proto"
)

// WalOpType op type
type WalOpType int

const (
	// WalSyncOp sync op
	WalSyncOp WalOpType = iota
	// WalWriteOp write op
	WalWriteOp
	// WalTruncateOp truncate op
	WalTruncateOp
	// AsyncOpChanLength op chan length
	AsyncOpChanLength = 1024 * 16
)

var (
	// UnSupportMarshalTypeFormat message
	UnSupportMarshalTypeFormat = "it is not support to marshal for type[%T]"
)

// WalOp Wal Operate
type WalOp struct {
	walOpType WalOpType
	index     uint64
	data      interface{}
}

// WalServiceImpl struct
type WalServiceImpl struct {
	sync.RWMutex
	walOption  *ConsensusWalOption
	wal        *wal.Log    // inner wal
	firstIndex uint64      // first index, which will update when TruncateFront
	lastIndex  uint64      // last index which will update when Write
	opChannel  chan *WalOp // operate channels
	mf         MarshalFunc
}

// NewWalService new service
func NewWalService(mf MarshalFunc, opFuncs ...ConsensusWalOptionFunc) (*WalServiceImpl, error) {
	walOption := NewDefaultConsensusWalOption()
	for _, walOptionFunc := range opFuncs {
		walOptionFunc(&walOption)
	}
	walService := &WalServiceImpl{
		walOption: &walOption,
	}
	if mf == nil {
		// use default marshal function
		mf = DefaultMarshalFunc
	}
	walService.mf = mf
	if len(walOption.walWritePath) > 0 {
		// need write，check mode
		if walOption.walWriteMode == NonWalWrite {
			return nil, fmt.Errorf("the write mode of wal is error")
		}
		// create wal
		walLog, err := wal.Open(walOption.walWritePath, nil)
		if err != nil {
			return nil, err
		}
		walService.wal = walLog
		// load
		walService.firstIndex, err = walLog.FirstIndex()
		if err != nil {
			return nil, err
		}
		walService.lastIndex, err = walLog.LastIndex()
		if err != nil {
			return nil, err
		}
	}
	if walOption.walWriteMode == AsyncWalWrite {
		walService.opChannel = make(chan *WalOp, AsyncOpChanLength)
		// async write
		go walService.asyncWriteListen()
	}
	return walService, nil
}

// WalMode wal mode
func (w *WalServiceImpl) WalMode() WalWriteMode {
	return w.walOption.walWriteMode
}

// Sync sync
func (w *WalServiceImpl) Sync() error {
	if w.isNon() {
		// do nothing
		return nil
	}
	if w.isAsync() {
		walOp := &WalOp{
			walOpType: WalSyncOp,
		}
		w.opChannel <- walOp
		return nil
	}
	return w.wal.Sync()
}

// LastIndex get last index
func (w *WalServiceImpl) LastIndex() (index uint64, err error) {
	w.RLock()
	defer w.RUnlock()
	return w.lastIndex, nil
}

//Write Wal Write
func (w *WalServiceImpl) Write(data interface{}) error {
	if w.isNon() {
		return nil
	}
	dataBytes := w.mf(data)
	w.Lock()
	defer w.Unlock()
	w.lastIndexIncrement()
	if w.isAsync() {
		walOp := &WalOp{
			walOpType: WalWriteOp,
			data:      data,
			index:     w.lastIndex,
		}
		w.opChannel <- walOp
		return nil
	}
	// marshal data
	return w.wal.Write(w.lastIndex, dataBytes)
}

// Read  data from file, which is not supported in async mode
func (w *WalServiceImpl) Read(index uint64) (data []byte, err error) {
	if w.isNon() {
		return nil, wal.ErrNotFound
	}
	return w.wal.Read(index)
}

// TruncateFront Truncate Wal from index
func (w *WalServiceImpl) TruncateFront(index uint64) error {
	if w.isNon() {
		return nil
	}
	w.Lock()
	defer w.Unlock()
	if w.isAsync() {
		walOp := &WalOp{
			walOpType: WalTruncateOp,
			index:     index,
		}
		w.opChannel <- walOp
		return nil
	}
	err := w.wal.TruncateFront(index)
	if err != nil {
		return err
	}
	// reset first index
	w.firstIndex, err = w.wal.FirstIndex()
	if err != nil {
		panic(err)
	}
	return nil
}

//Close  impl
func (w *WalServiceImpl) Close() error {
	w.Lock()
	defer w.Unlock()
	if w.isNon() {
		return nil
	}
	if w.isAsync() {
		// close the channel
		close(w.opChannel)
	}
	return w.wal.Close()
}

//asyncWriteListen
func (w *WalServiceImpl) asyncWriteListen() {
	for walOp := range w.opChannel {
		// handle the op of wal
		w.handleWalOp(walOp)
	}
}

//handle Wal Operate
func (w *WalServiceImpl) handleWalOp(op *WalOp) {
	if op.walOpType == WalSyncOp {
		_ = w.wal.Sync()
		return
	}
	if op.walOpType == WalWriteOp {
		dataBytes := w.mf(op.data)
		_ = w.wal.Write(op.index, dataBytes)
		return
	}
	if op.walOpType == WalTruncateOp {
		_ = w.wal.TruncateFront(op.index)
	}
}

//lastIndexIncrement
func (w *WalServiceImpl) lastIndexIncrement() {
	w.lastIndex++
}

//set walWriteMode NonWalWrite
func (w *WalServiceImpl) isNon() bool {
	return w.walOption.walWriteMode == NonWalWrite
}

//set walWriteMode AsyncWalWrite
func (w *WalServiceImpl) isAsync() bool {
	return w.walOption.walWriteMode == AsyncWalWrite
}

// WalService Wal Service
type WalService interface {
	// WalMode return wal write mode
	WalMode() WalWriteMode

	// Sync flush buffer into files
	Sync() error

	// LastIndex load the last index from wal file
	LastIndex() (index uint64, err error)

	// Write write the data into wal
	Write(data interface{}) error

	// Read read the data from wal
	Read(index uint64) (data []byte, err error)

	// TruncateFront truncate the data
	TruncateFront(index uint64) error

	// Close close the wal
	Close() error
}

// DefaultMarshalFunc default marshal func
func DefaultMarshalFunc(data interface{}) []byte {
	// check is []byte
	if msg, ok := data.([]byte); ok {
		return msg
	}
	// use pb
	msg, ok := data.(proto.Message)
	if !ok {
		// panic the type error
		panic(fmt.Errorf(UnSupportMarshalTypeFormat, data))
	}
	dataBytes, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return dataBytes
}
