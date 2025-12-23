/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package rpc

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"techtradechain.com/techtradechain/vm-docker-go/v2/utils"

	"techtradechain.com/techtradechain/logger/v2"

	"techtradechain.com/techtradechain/vm-docker-go/v2/config"
	"techtradechain.com/techtradechain/vm-docker-go/v2/pb/protogo"
	"go.uber.org/atomic"
)

type EventType int

type Event struct {
	id        uint64
	eventType EventType
}

const (
	connectionStopped EventType = iota
)

const (
	txSize               = 15000
	eventSize            = 100
	retryConnectDuration = 2 * time.Second
)

var clientMgrOnce sync.Once
var instance *ClientManager
var startErr error

type ClientManager struct {
	startOnce         sync.Once
	logger            *logger.CMLogger
	count             *atomic.Uint64 // tx count
	index             uint64         // client index
	config            *config.DockerVMConfig
	receiveChanLock   sync.RWMutex
	clientLock        sync.Mutex
	aliveClientMap    map[uint64]*CDMClient               // used to restore alive client
	txSendCh          chan *protogo.CDMMessage            // used to send tx to docker-go instance
	sysCallRespSendCh chan *protogo.CDMMessage            // used to receive message from docker-go
	receiveChMap      map[string]chan *protogo.CDMMessage // used to receive tx response from docker-go
	eventCh           chan *Event                         // used to receive event
	stop              bool
}

func NewClientManager(vmConfig *config.DockerVMConfig) *ClientManager {
	clientMgrOnce.Do(func() {
		instance = &ClientManager{
			logger:            logger.GetLogger(logger.MODULE_VM),
			count:             atomic.NewUint64(0),
			index:             1,
			config:            vmConfig,
			aliveClientMap:    make(map[uint64]*CDMClient),
			txSendCh:          make(chan *protogo.CDMMessage, txSize),
			sysCallRespSendCh: make(chan *protogo.CDMMessage, txSize*8),
			receiveChMap:      make(map[string]chan *protogo.CDMMessage),
			eventCh:           make(chan *Event, eventSize),
			stop:              false,
		}
	})

	return instance
}

func (cm *ClientManager) Start() error {
	cm.logger.Infof("start client manager")

	cm.startOnce.Do(func() {
		// 1. start all clients
		if err := cm.establishConnections(); err != nil {
			cm.logger.Errorf("fail to create client: %s", err)
			startErr = err
			return
		}

		// 2. start event listen
		go cm.listen()
	})

	return startErr
}

func (cm *ClientManager) GetUniqueTxKey(txId string) string {
	var sb strings.Builder
	nextCount := cm.count.Add(1)
	sb.WriteString(txId)
	sb.WriteString("#")
	sb.WriteString(strconv.FormatUint(nextCount, 10))
	return sb.String()
}

func (cm *ClientManager) GetTxSendCh() chan *protogo.CDMMessage {
	return cm.txSendCh
}

func (cm *ClientManager) GetSysCallRespSendCh() chan *protogo.CDMMessage {
	return cm.sysCallRespSendCh
}

func (cm *ClientManager) GetVMConfig() *config.DockerVMConfig {
	return cm.config
}

func (cm *ClientManager) PutEvent(event *Event) {
	cm.eventCh <- event
}

func (cm *ClientManager) PutTxRequest(txRequest *protogo.CDMMessage) {
	cm.logger.Debugf("[%s] put tx in send chan with length [%d]", txRequest.TxId, len(cm.txSendCh))
	cm.txSendCh <- txRequest
}

func (cm *ClientManager) PutSysCallResponse(sysCallResp *protogo.CDMMessage) {
	cm.sysCallRespSendCh <- sysCallResp
}

func (cm *ClientManager) RegisterReceiveChan(chainId, txId string, receiveCh chan *protogo.CDMMessage) error {
	cm.receiveChanLock.Lock()
	defer cm.receiveChanLock.Unlock()
	receiveChKey := utils.ConstructReceiveMapKey(chainId, txId)
	cm.logger.Debugf("register receive chan for [%s]", receiveChKey)

	_, ok := cm.receiveChMap[receiveChKey]
	if ok {
		cm.logger.Errorf("[%s] fail to register receive chan cause chan already registered", receiveChKey)
		return utils.ErrDuplicateTxId
	}

	cm.receiveChMap[receiveChKey] = receiveCh
	return nil
}

func (cm *ClientManager) GetReceiveChan(chainId, txId string) chan *protogo.CDMMessage {
	cm.receiveChanLock.RLock()
	defer cm.receiveChanLock.RUnlock()
	receiveChKey := utils.ConstructReceiveMapKey(chainId, txId)
	cm.logger.Debugf("get receive chan for [%s]", receiveChKey)
	return cm.receiveChMap[receiveChKey]
}

func (cm *ClientManager) GetAndDeleteReceiveChan(chainId, txId string) chan *protogo.CDMMessage {
	cm.receiveChanLock.Lock()
	defer cm.receiveChanLock.Unlock()
	receiveChKey := utils.ConstructReceiveMapKey(chainId, txId)
	cm.logger.Debugf("get receive chan for [%s] and delete", receiveChKey)
	receiveChan, ok := cm.receiveChMap[receiveChKey]
	if ok {
		delete(cm.receiveChMap, receiveChKey)
		return receiveChan
	}
	cm.logger.Warnf("cannot find receive chan for [%s] and return nil", receiveChKey)
	return nil
}

func (cm *ClientManager) DeleteReceiveChan(chainId, txId string) bool {
	cm.receiveChanLock.Lock()
	defer cm.receiveChanLock.Unlock()
	receiveChKey := utils.ConstructReceiveMapKey(chainId, txId)
	cm.logger.Debugf("[%s] delete receive chan", receiveChKey)
	_, ok := cm.receiveChMap[receiveChKey]
	if ok {
		delete(cm.receiveChMap, receiveChKey)
		return true
	}
	cm.logger.Debugf("[%s] delete receive chan fail, receive chan is already deleted", receiveChKey)
	return false
}

func (cm *ClientManager) listen() {
	cm.logger.Infof("client manager begin listen event")
	for {
		event := <-cm.eventCh
		switch event.eventType {
		case connectionStopped:
			cm.dropConnection(event)
			go cm.reconnect()
		default:
			cm.logger.Warnf("unknown event: %s", event)
		}
	}
}

func (cm *ClientManager) establishConnections() error {
	cm.clientLock.Lock()
	defer cm.clientLock.Unlock()
	cm.logger.Debugf("establish new connections")
	totalConnections := int(utils.GetMaxConnectionFromConfig(cm.GetVMConfig()))
	for i := 0; i < totalConnections; i++ {

		go func() {
			newIndex := cm.getNextIndex()
			newClient := NewCDMClient(newIndex, cm.logger, cm)

			for {
				if cm.stop {
					return
				}
				err := newClient.StartClient()
				if err == nil {
					break
				}
				cm.logger.Warnf("client[%d] connect fail, try reconnect...", newIndex)
				time.Sleep(retryConnectDuration)
			}
			cm.clientLock.Lock()
			cm.aliveClientMap[newIndex] = newClient
			cm.clientLock.Unlock()
		}()

	}
	return nil
}

func (cm *ClientManager) dropConnection(event *Event) {
	cm.clientLock.Lock()
	defer cm.clientLock.Unlock()
	cm.logger.Debugf("drop connection: %d", event.id)
	_, ok := cm.aliveClientMap[event.id]
	if ok {
		delete(cm.aliveClientMap, event.id)
	}
}

func (cm *ClientManager) CloseAllConnections() {
	cm.stop = true
	for _, client := range cm.aliveClientMap {
		client.StopSendRecv()
	}
}

func (cm *ClientManager) reconnect() {
	newIndex := cm.getNextIndex()
	newClient := NewCDMClient(newIndex, cm.logger, cm)

	for {
		if cm.stop {
			return
		}
		err := newClient.StartClient()
		if err == nil {
			break
		}
		cm.logger.Warnf("client[%d] connect fail, try reconnect...", newIndex)
		time.Sleep(retryConnectDuration)
	}

	cm.clientLock.Lock()
	cm.aliveClientMap[newIndex] = newClient
	cm.clientLock.Unlock()
}

func (cm *ClientManager) getNextIndex() uint64 {
	curIndex := cm.index
	cm.index++
	return curIndex
}

func (cm *ClientManager) NeedSendContractByteCode() bool {
	return !cm.config.DockerVMUDSOpen
}

func (cm *ClientManager) HasActiveConnections() bool {
	return len(cm.aliveClientMap) > 0
}
