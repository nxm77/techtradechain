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

// PeerIdTlsCertStore record the tls cert bytes of peer .
type PeerIdTlsCertStore struct {
	logger protocol.Logger
	lock   sync.RWMutex
	store  map[string][]byte
}

// NewPeerIdTlsCertStore .
func NewPeerIdTlsCertStore(logger protocol.Logger) *PeerIdTlsCertStore {
	return &PeerIdTlsCertStore{store: make(map[string][]byte), logger: logger}
}

// SetPeerTlsCert set the tls cert by the peerId
func (p *PeerIdTlsCertStore) SetPeerTlsCert(peerId string, tlsCert []byte) {
	p.lock.Lock()
	defer p.lock.Unlock()
	c, ok := p.store[peerId]
	if ok {
		if !bytes.Equal(c, tlsCert) {
			p.store[peerId] = tlsCert
		}
	} else {
		p.store[peerId] = tlsCert
	}
}

// RemoveByPeerId remove the cert by the peerId
func (p *PeerIdTlsCertStore) RemoveByPeerId(peerId string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.store, peerId)
}

// GetCertByPeerId get the cert by the peerId
func (p *PeerIdTlsCertStore) GetCertByPeerId(peerId string) []byte {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if cert, ok := p.store[peerId]; ok {
		return cert
	}
	return nil
}

// StoreCopy .
func (p *PeerIdTlsCertStore) StoreCopy() map[string][]byte {
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
