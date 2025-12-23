package rpc

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/atomic"

	protocol "techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-engine/v2/config"
	"techtradechain.com/techtradechain/vm-engine/v2/interfaces"
	"techtradechain.com/techtradechain/vm-engine/v2/pb/protogo"
	"techtradechain.com/techtradechain/vm-engine/v2/utils"
)

var (
	clientMgrOnce sync.Once
	mgrInstance   *ContractEngineClientManager
)

const (
	txSize               = 50000
	eventSize            = 100
	retryConnectDuration = 2 * time.Second
)

// ContractEngineClientManager manager all contract engine clients
type ContractEngineClientManager struct {
	startOnce      sync.Once
	logger         protocol.Logger
	count          atomic.Uint64 // tx count
	index          uint64        // client index
	config         *config.DockerVMConfig
	notifyLock     sync.RWMutex
	clientLock     sync.Mutex
	aliveClientMap map[uint64]*ContractEngineClient              // used to restore alive client
	txSendCh       chan *protogo.DockerVMMessage                 // used to send tx to docker-go instance
	byteCodeRespCh chan *protogo.DockerVMMessage                 // used to receive GetByteCode response from docker-go
	notify         map[string]func(msg *protogo.DockerVMMessage) // used to receive tx response from docker-go
	eventCh        chan *interfaces.Event                        // used to receive event
	stop           bool
}

// NewClientManager returns new client manager
func NewClientManager(
	logger protocol.Logger,
	vmConfig *config.DockerVMConfig,
) interfaces.ContractEngineClientMgr {

	clientMgrOnce.Do(func() {
		mgrInstance = &ContractEngineClientManager{
			startOnce:      sync.Once{},
			logger:         logger,
			count:          atomic.Uint64{},
			config:         vmConfig,
			notifyLock:     sync.RWMutex{},
			clientLock:     sync.Mutex{},
			aliveClientMap: make(map[uint64]*ContractEngineClient),
			txSendCh:       make(chan *protogo.DockerVMMessage, txSize),
			byteCodeRespCh: make(chan *protogo.DockerVMMessage, 8),
			notify:         make(map[string]func(msg *protogo.DockerVMMessage)),
			eventCh:        make(chan *interfaces.Event, eventSize),
			stop:           false,
		}
	})

	return mgrInstance
}

// Start establish connections
func (cm *ContractEngineClientManager) Start() error {
	cm.logger.Infof("start client manager")
	cm.logger.Infof("before start: alive conn %d", len(cm.aliveClientMap))

	var err error

	cm.startOnce.Do(func() {
		// 1. start all clients
		if err = cm.establishConnections(); err != nil {
			cm.logger.Errorf("fail to create client: %s", err)
			return
		}

		// 2. start event listen
		go cm.listen()
	})

	cm.logger.Infof("after start: alive conn %d, all connection established", len(cm.aliveClientMap))
	return err
}

// Stop all connectiosn
func (cm *ContractEngineClientManager) Stop() error {
	cm.closeAllConnections()
	return nil
}

func (cm *ContractEngineClientManager) closeAllConnections() {
	cm.stop = true
	for _, client := range cm.aliveClientMap {
		client.Stop()
	}
}

// === ForRuntimeInstance ===

// PutTxRequestWithNotify put tx request with notify into tx send channel
func (cm *ContractEngineClientManager) PutTxRequestWithNotify(
	txRequest *protogo.DockerVMMessage,
	chainId string,
	notify func(msg *protogo.DockerVMMessage),
) error {

	if err := cm.registerNotify(chainId, txRequest.TxId, notify); err != nil {
		return err
	}

	cm.txSendCh <- txRequest

	return nil
}

// PutByteCodeResp put butecode resp into bytecode resp channel
func (cm *ContractEngineClientManager) PutByteCodeResp(getByteCodeResp *protogo.DockerVMMessage) {
	cm.byteCodeRespCh <- getByteCodeResp
}

func (cm *ContractEngineClientManager) registerNotify(
	chainId,
	txId string,
	notify func(msg *protogo.DockerVMMessage),
) error {

	cm.notifyLock.Lock()
	defer cm.notifyLock.Unlock()
	notifyKey := utils.ConstructNotifyMapKey(chainId, txId)
	cm.logger.DebugDynamic(func() string {
		return fmt.Sprintf("register notify for [%s]", notifyKey)
	})

	_, ok := cm.notify[notifyKey]
	if ok {
		cm.logger.Errorf("[%s] fail to register notify, cause notify already registered", txId)
	}

	cm.notify[notifyKey] = notify
	return nil
}

// DeleteNotify delete notify
func (cm *ContractEngineClientManager) DeleteNotify(chainId, txId string) bool {
	cm.notifyLock.Lock()
	defer cm.notifyLock.Unlock()
	notifyKey := utils.ConstructNotifyMapKey(chainId, txId)
	cm.logger.DebugDynamic(func() string {
		return fmt.Sprintf("[%s] delete notify", notifyKey)
	})
	if _, ok := cm.notify[notifyKey]; ok {
		delete(cm.notify, notifyKey)
		return true
	}
	cm.logger.DebugDynamic(func() string {
		return fmt.Sprintf("[%s] delete notify fail, notify is already deleted", notifyKey)
	})
	return false
}

// GetUniqueTxKey returns unique tx key
func (cm *ContractEngineClientManager) GetUniqueTxKey(txId string) string {
	nextCount := cm.count.Add(1)
	return utils.ConstructUniqueTxKey(txId, strconv.FormatUint(nextCount, 10))
}

// NeedSendContractByteCode judge whether it need to send contract bytecode
func (cm *ContractEngineClientManager) NeedSendContractByteCode() bool {
	return cm.config.ConnectionProtocol != config.UDSProtocol
}

// HasActiveConnections returns the alive client map length
func (cm *ContractEngineClientManager) HasActiveConnections() bool {
	return len(cm.aliveClientMap) > 0
}

// GetVMConfig returns vm config
func (cm *ContractEngineClientManager) GetVMConfig() *config.DockerVMConfig {
	return cm.config
}

// GetTxSendChLen returns tx send chan length
func (cm *ContractEngineClientManager) GetTxSendChLen() int {
	return len(cm.txSendCh)
}

// GetByteCodeRespChLen returns bytecode resp chan length
func (cm *ContractEngineClientManager) GetByteCodeRespChLen() int {
	return len(cm.byteCodeRespCh)
}

// === forClient ===

// GetTxSendCh returns tx send channel
func (cm *ContractEngineClientManager) GetTxSendCh() chan *protogo.DockerVMMessage {
	return cm.txSendCh
}

// PutEvent put event into event channel
func (cm *ContractEngineClientManager) PutEvent(event *interfaces.Event) {
	cm.eventCh <- event
}

// GetByteCodeRespSendCh returns bytecode resp send ch
func (cm *ContractEngineClientManager) GetByteCodeRespSendCh() chan *protogo.DockerVMMessage {
	return cm.byteCodeRespCh
}

// GetReceiveNotify returns receive notify
func (cm *ContractEngineClientManager) GetReceiveNotify(chainId, txId string) func(msg *protogo.DockerVMMessage) {
	cm.notifyLock.RLock()
	defer cm.notifyLock.RUnlock()
	notifyKey := utils.ConstructNotifyMapKey(chainId, txId)
	cm.logger.DebugDynamic(func() string {
		return fmt.Sprintf("get notify for [%s]", notifyKey)
	})

	notify, ok := cm.notify[notifyKey]
	if !ok {
		cm.logger.Debugf("get receive notify[%s] failed, please check your key", notifyKey)
		return nil
	}

	return notify
}

func (cm *ContractEngineClientManager) listen() {
	cm.logger.Infof("client manager begin listen event")
	for {
		event := <-cm.eventCh
		switch event.EventType {
		case interfaces.EventType_ConnectionStopped:
			cm.dropConnection(event)
			go cm.reconnect()
		default:
			cm.logger.Warnf("unknown event: %s", event)
		}
	}
}

func (cm *ContractEngineClientManager) establishConnections() error {
	cm.logger.Debugf("establish new connections")
	totalConnections := int(utils.GetMaxConnectionFromConfig(cm.GetVMConfig()))
	var wg sync.WaitGroup
	for i := 0; i < totalConnections; i++ {
		wg.Add(1)
		go func() {
			newIndex := cm.getNextIndex()
			newClient := NewContractEngineClient(newIndex, cm.logger, cm)

			for {
				if cm.stop {
					return
				}
				if err := newClient.Start(); err != nil {
					cm.logger.Warnf("client[%d] connect fail, try again...", newIndex)
					time.Sleep(retryConnectDuration)
				} else {
					break
				}
			}
			cm.clientLock.Lock()
			cm.aliveClientMap[newIndex] = newClient
			cm.clientLock.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}

func (cm *ContractEngineClientManager) dropConnection(event *interfaces.Event) {
	cm.clientLock.Lock()
	defer cm.clientLock.Unlock()
	cm.logger.Debugf("drop connection: %d", event.Id)
	_, ok := cm.aliveClientMap[event.Id]
	if ok {
		delete(cm.aliveClientMap, event.Id)
	}
}

func (cm *ContractEngineClientManager) reconnect() {
	newIndex := cm.getNextIndex()
	newClient := NewContractEngineClient(newIndex, cm.logger, cm)

	for {
		if cm.stop {
			return
		}

		if err := newClient.Start(); err != nil {
			cm.logger.Warnf("client[%d] reconnect fail, %v, try again...", newIndex, err)
			time.Sleep(retryConnectDuration)
		} else {
			break
		}
	}

	cm.clientLock.Lock()
	cm.aliveClientMap[newIndex] = newClient
	cm.clientLock.Unlock()

}

func (cm *ContractEngineClientManager) getNextIndex() uint64 {

	cm.clientLock.Lock()
	defer cm.clientLock.Unlock()

	curIndex := cm.index
	cm.index++
	return curIndex
}
