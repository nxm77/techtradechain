/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package docker_go

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"techtradechain.com/techtradechain/logger/v2"
	"techtradechain.com/techtradechain/vm-docker-go/v2/pb/protogo"
)

type SysCallElapsedTime struct {
	OpType    protogo.CDMType
	StartTime int64
	//EndTime              int64
	TotalTime            int64
	StorageTimeInSysCall int64
}

func NewSysCallElapsedTime(opType protogo.CDMType, startTime int64, totalTime int64,
	storageTime int64) *SysCallElapsedTime {
	return &SysCallElapsedTime{
		OpType:               opType,
		StartTime:            startTime,
		TotalTime:            totalTime,
		StorageTimeInSysCall: storageTime,
	}
}

func (s *SysCallElapsedTime) ToString() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("%s start: %v, spend: %dμs, r/w store: %dμs; ",
		s.OpType.String(), time.Unix(s.StartTime/1e9, s.StartTime%1e9), s.TotalTime/1000, s.StorageTimeInSysCall/1000,
	)
}

type TxElapsedTime struct {
	TxId                  string
	StartTime             int64
	EndTime               int64
	TotalTime             int64
	SysCallCnt            int32
	SysCallTime           int64
	StorageTimeInSysCall  int64
	ContingentSysCallCnt  int32
	ContingentSysCallTime int64
	CrossCallCnt          int32
	CrossCallTime         int64
	SysCallList           []*SysCallElapsedTime
}

func NewTxElapsedTime(txId string, startTime int64) *TxElapsedTime {
	return &TxElapsedTime{
		TxId:        txId,
		StartTime:   startTime,
		SysCallList: make([]*SysCallElapsedTime, 0, 5),
	}
}

func (e *TxElapsedTime) ToString() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s spend time: %dμs, syscall: %dμs(%d), r/w store: %dμs, possible syscall: %dμs(%d)"+
		"cross contract: %dμs(%d)",
		e.TxId, e.TotalTime/1000, e.SysCallTime/1000, e.SysCallCnt, e.StorageTimeInSysCall/1000,
		e.ContingentSysCallTime/1000, e.ContingentSysCallCnt, e.CrossCallTime/1000, e.CrossCallCnt,
	)
}

func (e *TxElapsedTime) PrintSysCallList() string {
	if e.SysCallList == nil {
		return "no syscalls"
	}
	var sb strings.Builder
	sb.WriteString(e.TxId + ": ")
	for _, sysCallTime := range e.SysCallList {
		sb.WriteString(sysCallTime.ToString())
	}
	return sb.String()
}

// todo add lock (maybe do not need)
func (e *TxElapsedTime) AddSysCallElapsedTime(sysCallElapsedTime *SysCallElapsedTime) {
	if sysCallElapsedTime == nil {
		return
	}

	switch sysCallElapsedTime.OpType {
	case protogo.CDMType_CDM_TYPE_GET_BYTECODE, protogo.CDMType_CDM_TYPE_GET_CONTRACT_NAME:
		e.ContingentSysCallCnt++
		e.ContingentSysCallTime += sysCallElapsedTime.TotalTime
		e.StorageTimeInSysCall += sysCallElapsedTime.StorageTimeInSysCall
	case protogo.CDMType_CDM_TYPE_GET_STATE, protogo.CDMType_CDM_TYPE_GET_BATCH_STATE,
		protogo.CDMType_CDM_TYPE_CREATE_KV_ITERATOR, protogo.CDMType_CDM_TYPE_CONSUME_KV_ITERATOR,
		protogo.CDMType_CDM_TYPE_CREATE_KEY_HISTORY_ITER, protogo.CDMType_CDM_TYPE_CONSUME_KEY_HISTORY_ITER,
		protogo.CDMType_CDM_TYPE_GET_SENDER_ADDRESS:
		e.SysCallCnt++
		e.SysCallTime += sysCallElapsedTime.TotalTime
		e.StorageTimeInSysCall += sysCallElapsedTime.StorageTimeInSysCall
	default:
		return
	}
}

// todo add lock (maybe do not need)
func (e *TxElapsedTime) AddToSysCallList(sysCallElapsedTime *SysCallElapsedTime) {
	if sysCallElapsedTime == nil {
		return
	}

	e.SysCallList = append(e.SysCallList, sysCallElapsedTime)
}

func (e *TxElapsedTime) Add(t *TxElapsedTime) {
	if t == nil {
		return
	}

	t.TotalTime += t.TotalTime

	e.SysCallCnt += t.SysCallCnt
	e.SysCallTime += t.SysCallTime
	e.StorageTimeInSysCall += t.StorageTimeInSysCall

	e.ContingentSysCallCnt += t.ContingentSysCallCnt
	e.ContingentSysCallTime += t.ContingentSysCallTime

	e.CrossCallCnt += t.CrossCallCnt
	e.CrossCallTime += t.CrossCallTime
}

func (e *TxElapsedTime) AddContingentSysCall(spend int64) {
	e.ContingentSysCallCnt++
	e.ContingentSysCallTime += spend

}

type BlockElapsedTime struct {
	txs []*TxElapsedTime
}

// todo add lock
func (b *BlockElapsedTime) AddTxElapsedTime(t *TxElapsedTime) {
	b.txs = append(b.txs, t)
}

func (b *BlockElapsedTime) ToString() string {
	if b == nil {
		return ""
	}
	txTotal := NewTxElapsedTime("", 0)
	for _, tx := range b.txs {
		txTotal.Add(tx)
	}
	return txTotal.ToString()
}

type RequestMgr struct {
	RequestMap map[string]*BlockElapsedTime
	lock       sync.RWMutex
	logger     *logger.CMLogger
}

func NewRequestMgr() *RequestMgr {
	return &RequestMgr{
		RequestMap: make(map[string]*BlockElapsedTime),
		logger:     logger.GetLogger(logger.MODULE_RPC),
	}
}

func (r *RequestMgr) PrintBlockElapsedTime(requestId string) string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	blockElapsedTime := r.RequestMap[requestId]
	return blockElapsedTime.ToString()
}

func (r *RequestMgr) AddRequest(requestId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.RequestMap[requestId] == nil {
		r.RequestMap[requestId] = &BlockElapsedTime{}
		return
	}
	r.logger.Warnf("receive duplicated request: %s", requestId)
}

func (r *RequestMgr) RemoveRequest(requestId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.RequestMap, requestId)
}

// AddTx if add tx to tx request map need lock
func (r *RequestMgr) AddTx(requestId string, txTime *TxElapsedTime) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.RequestMap[requestId] == nil {
		return
	}
	r.RequestMap[requestId].AddTxElapsedTime(txTime)
}
