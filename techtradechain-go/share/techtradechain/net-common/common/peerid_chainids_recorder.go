/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"techtradechain.com/techtradechain/protocol/v2"

	"sync"
)

// PeerIdChainIdsRecorder record the chain ids of peer .
type PeerIdChainIdsRecorder struct {
	logger protocol.Logger
	lock   sync.RWMutex
	//newTlsPeerChainIdsNotifyChanHandling bool
	//removeTlsPeerNotifyChanHandling      bool
	records   map[string]*StringMapList
	onAddC    chan<- string
	onRemoveC chan<- string
}

//NewPeerIdChainIdsRecorder new the peerids recorder
func NewPeerIdChainIdsRecorder(logger protocol.Logger) *PeerIdChainIdsRecorder {
	return &PeerIdChainIdsRecorder{records: make(map[string]*StringMapList), logger: logger}
}

// OnAddNotifyC if the peer add a chain, notify the chan
func (pcr *PeerIdChainIdsRecorder) OnAddNotifyC(onAddC chan<- string) {
	pcr.lock.Lock()
	defer pcr.lock.Unlock()
	pcr.onAddC = onAddC
}

// OnRemoveNotifyC if the peer remove the chain, notify the chan
func (pcr *PeerIdChainIdsRecorder) OnRemoveNotifyC(onRemoveC chan<- string) {
	pcr.lock.Lock()
	defer pcr.lock.Unlock()
	pcr.onRemoveC = onRemoveC
}

// AddPeerChainId add the chainId to the PeerIdChainIdsRecorder
func (pcr *PeerIdChainIdsRecorder) AddPeerChainId(peerId string, chainId string) bool {
	pcr.lock.Lock()
	defer pcr.lock.Unlock()
	mapList, ok := pcr.records[peerId]
	if !ok {
		pcr.records[peerId] = NewStringMapList()
		mapList = pcr.records[peerId]
	}
	result := mapList.Add(chainId)
	if result && pcr.onAddC != nil {
		pcr.onAddC <- peerId + "<-->" + chainId
	}
	return result
}

// RemovePeerChainId remove the chainId from the PeerIdChainIdsRecorder
func (pcr *PeerIdChainIdsRecorder) RemovePeerChainId(peerId string, chainId string) bool {
	pcr.lock.Lock()
	defer pcr.lock.Unlock()
	mapList, ok := pcr.records[peerId]
	if !ok {
		return false
	}
	result := mapList.Remove(chainId)
	if result && pcr.onRemoveC != nil {
		pcr.onRemoveC <- peerId + "<-->" + chainId
	}
	return result
}

// RemoveAllByPeerId remove all chainIds from the PeerIdChainIdsRecorder
func (pcr *PeerIdChainIdsRecorder) RemoveAllByPeerId(peerId string) bool {
	pcr.lock.Lock()
	defer pcr.lock.Unlock()
	chains, ok := pcr.records[peerId]
	if ok {
		if pcr.onRemoveC != nil {
			for chainId := range chains.mapList {
				pcr.onRemoveC <- peerId + "<-->" + chainId
			}
		}
		delete(pcr.records, peerId)
		return true
	}
	return false
}

// IsPeerBelongToChain is the peer belong to the chain
func (pcr *PeerIdChainIdsRecorder) IsPeerBelongToChain(peerId string, chainId string) bool {
	pcr.lock.RLock()
	defer pcr.lock.RUnlock()
	m, ok := pcr.records[peerId]
	if ok {
		return m.Contains(chainId)
	}
	return false
}

// PeerIdsOfChain all peerIds of the chain
func (pcr *PeerIdChainIdsRecorder) PeerIdsOfChain(chainId string) []string {
	pcr.lock.RLock()
	defer pcr.lock.RUnlock()
	result := make([]string, 0)
	for peerId, mapList := range pcr.records {
		if mapList.Contains(chainId) {
			result = append(result, peerId)
		}
	}
	return result
}

// PeerIdsOfNoChain all peerIds not in any chain
func (pcr *PeerIdChainIdsRecorder) PeerIdsOfNoChain() []string {
	pcr.lock.RLock()
	defer pcr.lock.RUnlock()
	result := make([]string, 0)
	for peerId, mapList := range pcr.records {
		if mapList.Size() == 0 {
			result = append(result, peerId)
		}
	}
	return result
}
