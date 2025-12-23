/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import (
	"errors"
	"regexp"
	"strings"
	"sync"

	"techtradechain.com/techtradechain/protocol/v2"
)

const (
	tlsCertIdFlag     = ".tls."
	signCertIdFlag    = ".sign."
	signCertIdPattern = ".+\\.sign\\..+"
)

// CertIdPeerIdMapper mapped cert id with peer id.
type CertIdPeerIdMapper struct {
	logger protocol.Logger
	lock   sync.RWMutex
	mapper map[string]string
}

// NewCertIdPeerIdMapper create a new CertIdPeerIdMapper instance.
func NewCertIdPeerIdMapper(logger protocol.Logger) *CertIdPeerIdMapper {
	return &CertIdPeerIdMapper{mapper: make(map[string]string), logger: logger}
}

// Add a record mapping cert id with peer id.
func (c *CertIdPeerIdMapper) Add(certId string, peerId string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.mapper[certId]; ok {
		return
	}
	c.mapper[certId] = peerId
}

// RemoveByPeerId remove all records mapped with given peerId.
func (c *CertIdPeerIdMapper) RemoveByPeerId(peerId string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for certId, pid := range c.mapper {
		if pid == peerId {
			delete(c.mapper, certId)
		}
	}
}

// FindPeerIdByCertId will return a peer id if the given cert id has mapped with a peer id .
func (c *CertIdPeerIdMapper) FindPeerIdByCertId(certId string) (string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	certId = parseSignCertIdToTlsCertId(certId)
	peerId, ok := c.mapper[certId]
	if !ok {
		c.logger.Debugf("cert id not mapping(certId:%s)", certId)
		return "", errors.New("cert id not mapping")
	}
	return peerId, nil
}

func parseSignCertIdToTlsCertId(certId string) string {
	ok, err := regexp.Match(signCertIdPattern, []byte(certId))
	if err != nil {
		return certId
	}
	if ok {
		return strings.ReplaceAll(certId, signCertIdFlag, tlsCertIdFlag)
	}
	return certId
}

// GetAll .
// @Description: return all cert info
// @receiver c
// @return ${return_type}
// GetAll .
func (c *CertIdPeerIdMapper) GetAll() []string {
	res := make([]string, 0)
	c.lock.Lock()
	defer c.lock.Unlock()
	for certId, pid := range c.mapper {
		res = append(res, certId, pid)
	}
	return res
}
