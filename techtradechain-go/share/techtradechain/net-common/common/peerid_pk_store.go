/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"bytes"
	"sync"

	"techtradechain.com/techtradechain/protocol/v2"
)

// PeerIdPubKeyStore record the public key bytes of peer .
type PeerIdPubKeyStore struct {
	logger protocol.Logger
	lock   sync.RWMutex
	store  map[string][]byte
}

// NewPeerIdPubKeyStore .
func NewPeerIdPubKeyStore(logger protocol.Logger) *PeerIdPubKeyStore {
	return &PeerIdPubKeyStore{store: make(map[string][]byte), logger: logger}
}

// SetPeerPubKey Set the peer public key
func (p *PeerIdPubKeyStore) SetPeerPubKey(peerId string, pubKey []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()
	c, ok := p.store[peerId]
	if ok {
		if !bytes.Equal(c, pubKey) {
			p.store[peerId] = pubKey
		}
	} else {
		p.store[peerId] = pubKey
	}
}

// RemoveByPeerId remove the public key by the peerId
func (p *PeerIdPubKeyStore) RemoveByPeerId(peerId string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	//if _, ok := p.store[peerId]; ok {
	delete(p.store, peerId)
	//}
}

// GetCertByPeerId get the public key by the peerId
func (p *PeerIdPubKeyStore) GetCertByPeerId(peerId string) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if cert, ok := p.store[peerId]; ok {
		return cert
	}
	return nil
}

// StoreCopy copy
func (p *PeerIdPubKeyStore) StoreCopy() map[string][]byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	newMap := make(map[string][]byte)
	for pid := range p.store {
		temp := p.store[pid]
		newBytes := make([]byte, len(temp))
		copy(newBytes, temp)
		newMap[pid] = newBytes
	}
	return newMap
}
