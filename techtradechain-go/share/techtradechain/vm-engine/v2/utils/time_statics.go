package utils

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	protocol "techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-engine/v2/config"
	"techtradechain.com/techtradechain/vm-engine/v2/pb/protogo"
)

// SysCallDuration .
type SysCallDuration struct {
	OpType          protogo.DockerVMType
	StartTime       int64
	TotalDuration   int64
	StorageDuration int64
}

// NewSysCallDuration construct a SysCallDuration
func NewSysCallDuration(opType protogo.DockerVMType, startTime int64, totalTime int64,
	storageTime int64) *SysCallDuration {
	return &SysCallDuration{
		OpType:          opType,
		StartTime:       startTime,
		TotalDuration:   totalTime,
		StorageDuration: storageTime,
	}
}

// ToString .
func (s *SysCallDuration) ToString() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("<%s start: %v, spend: %dms, store: %dms> ",
		s.OpType.String(), time.Unix(s.StartTime/1e9, s.StartTime%1e9), s.TotalDuration/1e6, s.StorageDuration/1e6,
	)
}

// AddStorageDuration .
func (s *SysCallDuration) AddStorageDuration(nanos int64) {
	if s == nil {
		return
	}
	s.StorageDuration += nanos
}

// TxDuration .
type TxDuration struct {
	OriginalTxId                string
	TxId                        string
	StartTime                   int64
	EndTime                     int64
	TotalDuration               int64
	SysCallCnt                  int32
	SysCallDuration             int64
	StorageDuration             int64
	LoadContractSysCallCnt      int32
	LoadContractSysCallDuration int64
	CrossCallCnt                int32
	CrossCallDuration           int64
	SysCallList                 []*SysCallDuration
	CrossCallList               []*TxDuration
	CurrDurationStack           *list.List
}

// NewTxDuration .
func NewTxDuration(originalTxId, txId string, startTime int64) *TxDuration {
	return &TxDuration{
		OriginalTxId: originalTxId,
		TxId:         txId,
		StartTime:    startTime,
		SysCallList:  make([]*SysCallDuration, 0, 8),
	}
}

// ToString .
func (e *TxDuration) ToString() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s spend time: %dms, syscall: %dms(count: %d), r/w store: %dms, "+
		"load contract syscall: %dms(count: %d), cross contract: %dms(count: %d)",
		e.TxId, e.TotalDuration/1e6, e.SysCallDuration/1e6, e.SysCallCnt, e.StorageDuration/1e6,
		e.LoadContractSysCallDuration/1e6, e.LoadContractSysCallCnt, e.CrossCallDuration/1e6, e.CrossCallCnt,
	)
}

// PrintSysCallList print tx duration
func (e *TxDuration) PrintSysCallList() string {
	if e == nil {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(e.ToString())

	// log sys call list
	if e.SysCallList == nil {
		return sb.String()
	}
	sb.WriteString(" syscalls:[")
	for _, sysCallTime := range e.SysCallList {
		sb.WriteString(sysCallTime.ToString())
	}
	sb.WriteString("]")

	// log cross call list
	if e.CrossCallList == nil {
		return sb.String()
	}
	sb.WriteString(" crosscalls:[")
	for _, crossCall := range e.CrossCallList {
		sb.WriteString(crossCall.PrintSysCallList())
	}
	sb.WriteString("]")

	return sb.String()
}

// StartSysCall start new sys call
func (e *TxDuration) StartSysCall(msgType protogo.DockerVMType) *SysCallDuration {
	//duration, _ := sysCallPool.Get().(*SysCallDuration)
	duration := &SysCallDuration{}
	duration.OpType = msgType
	duration.StartTime = time.Now().UnixNano()
	duration.StorageDuration = 0
	duration.TotalDuration = 0
	e.SysCallList = append(e.SysCallList, duration)

	return duration

}

// AddCrossDuration start new sys call
func (e *TxDuration) AddCrossDuration(duration *TxDuration) {
	currDuration := e.GetCurrDuration()
	currDuration.CrossCallList = append(currDuration.CrossCallList, duration)

	e.CurrDurationStack.PushBack(duration)

}

// AddLatestStorageDuration add storage time to latest sys call
func (e *TxDuration) AddLatestStorageDuration(duration int64) error {
	latestSysCall, err := e.GetLatestSysCall()
	if err != nil {
		return fmt.Errorf("failed to get latest sys call, %v", err)
	}
	latestSysCall.StorageDuration += duration
	return nil
}

// EndSysCall close new sys call
func (e *TxDuration) EndSysCall(msg *protogo.DockerVMMessage) error {
	latestSysCall, err := e.GetLatestSysCall()
	if err != nil {
		return fmt.Errorf("failed to get latest sys call, %v", err)
	}
	latestSysCall.TotalDuration = time.Since(time.Unix(0, latestSysCall.StartTime)).Nanoseconds()
	e.addSysCallDuration(latestSysCall)
	return nil
}

// GetLatestSysCall returns latest sys call
func (e *TxDuration) GetLatestSysCall() (*SysCallDuration, error) {
	if len(e.SysCallList) == 0 {
		return nil, errors.New("sys call list length == 0")
	}
	return e.SysCallList[len(e.SysCallList)-1], nil
}

// todo add lock (maybe do not need)
// addSysCallDuration add the count of system calls and the duration of system calls to the total record
func (e *TxDuration) addSysCallDuration(duration *SysCallDuration) {
	if duration == nil {
		return
	}
	switch duration.OpType {
	case protogo.DockerVMType_GET_BYTECODE_REQUEST:
		// get bytecode are recorded separately, which is different from syscall
		e.LoadContractSysCallCnt++
		e.LoadContractSysCallDuration += duration.TotalDuration
		e.StorageDuration += duration.StorageDuration
	case protogo.DockerVMType_GET_STATE_REQUEST, protogo.DockerVMType_GET_BATCH_STATE_REQUEST,
		protogo.DockerVMType_CREATE_KV_ITERATOR_REQUEST, protogo.DockerVMType_CONSUME_KV_ITERATOR_REQUEST,
		protogo.DockerVMType_CREATE_KEY_HISTORY_ITER_REQUEST, protogo.DockerVMType_CONSUME_KEY_HISTORY_ITER_REQUEST,
		protogo.DockerVMType_GET_SENDER_ADDRESS_REQUEST:
		// record all syscalls except cross contract calls and get bytecode
		e.SysCallCnt++
		e.SysCallDuration += duration.TotalDuration
		e.StorageDuration += duration.StorageDuration
	case protogo.DockerVMType_CALL_CONTRACT_REQUEST:
		// cross contract calls are recorded separately, which is different from syscall
		e.CrossCallCnt++
		e.CrossCallDuration += duration.TotalDuration
	default:
		return
	}
}

// Add the param txDuration to self
// just add the root duration,
// the duration of child nodes will be synchronized to the root node at the end of statistics
func (e *TxDuration) Add(txDuration *TxDuration) {
	if txDuration == nil {
		return
	}

	e.TotalDuration += txDuration.TotalDuration

	e.SysCallCnt += txDuration.SysCallCnt
	e.SysCallDuration += txDuration.SysCallDuration
	e.StorageDuration += txDuration.StorageDuration

	e.LoadContractSysCallCnt += txDuration.LoadContractSysCallCnt
	e.LoadContractSysCallDuration += txDuration.LoadContractSysCallDuration

	e.CrossCallCnt += txDuration.CrossCallCnt
	e.CrossCallDuration += txDuration.CrossCallDuration
}

// GetCurrDuration get current duration
func (e *TxDuration) GetCurrDuration() *TxDuration {
	// this stack need to be initialized during add root tx duration
	if e.CurrDurationStack == nil {
		// todo not root duration need to  add err log
		return nil
	}

	if e.CurrDurationStack.Len() == 0 {
		return e
	}

	duration, ok := e.CurrDurationStack.Back().Value.(*TxDuration)
	if ok {
		return duration
	}
	return nil
}

// BlockTxsDuration record the duration of all transactions in a block
type BlockTxsDuration struct {
	txs map[string][]*TxDuration
}

// ToString .
func (b *BlockTxsDuration) ToString() string {
	if b == nil {
		return ""
	}
	execTxCnt := 0
	txTotal := NewTxDuration("", "", 0)
	for _, tx := range b.txs {
		execTxCnt += len(tx)
		for _, t := range tx {
			txTotal.Add(t)
		}
	}
	return fmt.Sprintf("total exec tx count: %d ", execTxCnt) + txTotal.ToString()
}

// BlockTxsDurationMgr record the txDuration of each block
type BlockTxsDurationMgr struct {
	blockDurations map[string]*BlockTxsDuration
	lock           sync.RWMutex
	logger         protocol.Logger
}

// NewBlockTxsDurationMgr construct a BlockTxsDurationMgr
func NewBlockTxsDurationMgr(logger protocol.Logger) *BlockTxsDurationMgr {
	return &BlockTxsDurationMgr{
		blockDurations: make(map[string]*BlockTxsDuration),
		logger:         logger,
	}
}

// PrintBlockTxsDuration returns the duration of the specified block
func (r *BlockTxsDurationMgr) PrintBlockTxsDuration(id string) string {
	r.lock.RLock()
	defer r.lock.RUnlock()
	durations := r.blockDurations[id]
	return durations.ToString()
}

// PrintDetailBlockTxsDuration returns the duration of the specified block
func (r *BlockTxsDurationMgr) PrintDetailBlockTxsDuration(id string) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	blockDurations := r.blockDurations[id]
	for uniqueTxID, txDurations := range blockDurations.txs {
		for _, txDuration := range txDurations {
			r.logger.Debugf("uniqueTxID: %s, txDuration: %s", uniqueTxID, txDuration.ToString())
		}
	}
}

// AddBlockTxsDuration .
func (r *BlockTxsDurationMgr) AddBlockTxsDuration(id string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.blockDurations[id] == nil {
		r.blockDurations[id] = &BlockTxsDuration{}
		return
	}
	r.logger.Warnf("receive duplicated block, fingerprint: %s", id)
}

// RemoveBlockTxsDuration .
func (r *BlockTxsDurationMgr) RemoveBlockTxsDuration(id string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.blockDurations, id)
}

// AddTx if add tx to block map need lock
func (r *BlockTxsDurationMgr) AddTx(id string, txTime *TxDuration) {
	// root tx duration create
	txTime.CurrDurationStack = list.New()

	r.lock.Lock()
	defer r.lock.Unlock()
	if r.blockDurations[id] == nil {
		return
	}
	b := r.blockDurations[id]
	if b.txs == nil {
		b.txs = make(map[string][]*TxDuration)
	}

	b.txs[txTime.OriginalTxId] = append(b.txs[txTime.OriginalTxId], txTime)

}

// AddCrossTx add cross tx to block map need lock
func (r *BlockTxsDurationMgr) AddCrossTx(id string, txTime *TxDuration) {
	duration := r.getRootTxDuration(id, txTime.OriginalTxId)
	if duration == nil {
		return
	}
	duration.AddCrossDuration(txTime)
}

// FinishTx .
func (r *BlockTxsDurationMgr) getRootTxDuration(id string, txId string) *TxDuration {
	r.lock.Lock()
	defer r.lock.Unlock()
	b, ok := r.blockDurations[id]
	if !ok {
		r.logger.Debugf("block duration not exist, fingerprint: %s", id)
		return nil
	}

	if b.txs == nil {
		r.logger.Debugf("tx durations not exist, fingerprint: %s", id)
		b.txs = make(map[string][]*TxDuration)
	}

	if len(b.txs[txId]) == 0 {
		// 交易统计不存在
		r.logger.Debugf("tx duration not exist, tx id: %s", txId)
		return nil
	}

	return b.txs[txId][len(b.txs[txId])-1]

}

// FinishTx .
func (r *BlockTxsDurationMgr) FinishTx(id string, txTime *TxDuration) {

	rootTxDuration := r.getRootTxDuration(id, txTime.OriginalTxId)
	if rootTxDuration == nil {
		return
	}

	if rootTxDuration.CurrDurationStack.Len() == 0 {
		if !config.VMConfig.Slow.Disable && rootTxDuration.TotalDuration > 1e6*int64(config.VMConfig.Slow.TxTime) {
			r.logger.InfoDynamic(func() string {
				return "slow tx duration: " + rootTxDuration.PrintSysCallList()
			})
		}
		return
	}

	// CurrDurationStack pop last
	rootTxDuration.CurrDurationStack.Remove(rootTxDuration.CurrDurationStack.Back())

	var parentDuration *TxDuration
	var ok bool

	if rootTxDuration.CurrDurationStack.Len() == 0 {
		parentDuration = rootTxDuration
	} else {
		parentDuration, ok = rootTxDuration.CurrDurationStack.Back().Value.(*TxDuration)
		if !ok {
			r.logger.Infof("type assertion failed, %v", rootTxDuration.CurrDurationStack.Back().Value)
			return
		}
	}

	parentDuration.SysCallCnt += txTime.SysCallCnt
	parentDuration.SysCallDuration += txTime.SysCallDuration

	parentDuration.LoadContractSysCallCnt += txTime.LoadContractSysCallCnt
	parentDuration.LoadContractSysCallDuration += txTime.LoadContractSysCallDuration

	parentDuration.CrossCallCnt += txTime.CrossCallCnt
	parentDuration.CrossCallDuration += txTime.CrossCallDuration

}

// EnterNextStep enter next duration tx step
func EnterNextStep(msg *protogo.DockerVMMessage, stepType protogo.StepType, getStr func() string) {
	if config.VMConfig.Slow.Disable {
		return
	}
	if stepType != protogo.StepType_RUNTIME_PREPARE_TX_REQUEST {
		endTxStep(msg)
	}
	addTxStep(msg, stepType, getStr())
	if stepType == protogo.StepType_RUNTIME_HANDLE_TX_RESPONSE {
		endTxStep(msg)
	}
}

func addTxStep(msg *protogo.DockerVMMessage, stepType protogo.StepType, log string) {
	//stepDur, _ := TxStepPool.Get().(*protogo.StepDuration)
	//stepDur.Type = stepType
	//stepDur.StartTime = time.Now().UnixNano()
	//stepDur.Msg = log
	//stepDur.StepDuration = 0
	//stepDur.UntilDuration = 0
	msg.StepDurations = append(msg.StepDurations, &protogo.StepDuration{
		Type:      stepType,
		StartTime: time.Now().UnixNano(),
		Msg:       log,
	})
}

func endTxStep(msg *protogo.DockerVMMessage) {
	if len(msg.StepDurations) == 0 {
		return
	}
	stepLen := len(msg.StepDurations)
	currStep := msg.StepDurations[stepLen-1]
	firstStep := msg.StepDurations[0]
	currStep.UntilDuration = time.Since(time.Unix(0, firstStep.StartTime)).Nanoseconds()
	currStep.StepDuration = time.Since(time.Unix(0, currStep.StartTime)).Nanoseconds()
}

// PrintTxSteps print all duration tx steps
func PrintTxSteps(msg *protogo.DockerVMMessage) string {
	var sb strings.Builder
	for _, step := range msg.StepDurations {
		sb.WriteString(fmt.Sprintf("<step: %q, start time: %v, step cost: %vms, until cost: %vms, msg: %s> ",
			step.Type, time.Unix(0, step.StartTime),
			time.Duration(step.StepDuration).Seconds()*1000,
			time.Duration(step.UntilDuration).Seconds()*1000,
			step.Msg))
	}
	return sb.String()
}

// PrintTxStepsWithTime print all duration tx steps with time limt
func PrintTxStepsWithTime(msg *protogo.DockerVMMessage) (string, bool) {
	if len(msg.StepDurations) == 0 {
		return "", false
	}
	lastStep := msg.StepDurations[len(msg.StepDurations)-1]
	var sb strings.Builder
	if lastStep.UntilDuration > time.Second.Microseconds()*int64(config.VMConfig.Slow.TxTime) {
		sb.WriteString("slow tx overall: ")
		sb.WriteString(PrintTxSteps(msg))
		return sb.String(), true
	}
	for _, step := range msg.StepDurations {
		if step.StepDuration > time.Second.Microseconds()*int64(config.VMConfig.Slow.StepTime) {
			sb.WriteString(fmt.Sprintf("slow tx at step %q, step cost: %vms: ",
				step.Type, time.Duration(step.StepDuration).Seconds()*1000))
			sb.WriteString(PrintTxSteps(msg))
			return sb.String(), true
		}
	}
	return "", false
}

// PrintAllBlockTxsDuration returns the duration of the specified block
func (r *BlockTxsDurationMgr) PrintAllBlockTxsDuration(id string) string {
	r.lock.RLock()
	durations := r.blockDurations[id]
	r.lock.RUnlock()

	jsonBytes, err := json.Marshal(durations.txs)
	if err != nil {
		r.logger.Warn("Error marshaling to JSON:", err)
		return ""
	}
	jsonString := string(jsonBytes)

	return jsonString
}
