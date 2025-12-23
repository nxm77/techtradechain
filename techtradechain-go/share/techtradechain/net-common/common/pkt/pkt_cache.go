/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkt

import (
	"techtradechain.com/techtradechain/net-common/utils"

	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultTimeout              = 10
	defaultCheckTimeoutInterval = 5 * time.Second
	cacheKeySeparator           = "::"
)

// FullPktResult wraps the sender string and the Pkt list of a msg payload whose seq is the same value.
type FullPktResult struct {
	Sender  string
	PktList []*Pkt
}

// Cache for collecting Pkt of payloads.
// If all Pkt belong to the same payload collecting finished,
// a FullPktResult contains sender id string and all Pkt list will be push to fullPktNotifyC.
type Cache struct {
	once                sync.Once
	mu                  sync.RWMutex
	statMap             map[string]*uint32 // map msg seq to count of pkt received
	pktMap              map[string][]*Pkt  // map msg seq to pkt list
	receiveFirstTimeMap map[string]int64   // map msg seq to receive first timestamp

	timeoutInSecond      int64
	checkTimeoutInterval time.Duration // in second
	ticker               *time.Ticker

	fullPktNotifyC chan *FullPktResult
	closeC         chan struct{}
}

// NewPktCache create a new Cache instance.
func NewPktCache() *Cache {
	c := &Cache{
		once:                 sync.Once{},
		mu:                   sync.RWMutex{},
		statMap:              make(map[string]*uint32),
		pktMap:               make(map[string][]*Pkt),
		receiveFirstTimeMap:  make(map[string]int64),
		timeoutInSecond:      defaultTimeout,
		checkTimeoutInterval: defaultCheckTimeoutInterval,
		ticker:               nil,
		fullPktNotifyC:       make(chan *FullPktResult),
		closeC:               make(chan struct{}),
	}
	return c
}

// Run a goroutine for checking expire Pkt.
func (c *Cache) Run() {
	c.once.Do(func() {
		go c.loop()
	})
}

func (c *Cache) loop() {
	c.ticker = time.NewTicker(c.checkTimeoutInterval)
	for {
		select {
		case <-c.ticker.C:
			now := utils.CurrentTimeMillisSeconds()
			timeoutSeq := make([]string, 0)
			c.mu.RLock()
			for s, u := range c.receiveFirstTimeMap {
				expireTime := u + c.timeoutInSecond*1000
				if expireTime < now {
					//timeout
					timeoutSeq = append(timeoutSeq, s)
				}
			}
			c.mu.RUnlock()
			for i := range timeoutSeq {
				s := timeoutSeq[i]
				c.mu.Lock()
				delete(c.statMap, s)
				delete(c.pktMap, s)
				delete(c.receiveFirstTimeMap, s)
				c.mu.Unlock()
			}
		case <-c.closeC:
			c.ticker.Stop()
			return
		}
	}
}

// Close the cache.
func (c *Cache) Close() {
	close(c.closeC)
}

// FullPktC return the fullPktNotifyC of Cache.
func (c *Cache) FullPktC() <-chan *FullPktResult {
	return c.fullPktNotifyC
}

func (c *Cache) createKey(sender string, p *Pkt) string {
	var builder strings.Builder
	builder.WriteString(sender)
	builder.WriteString(cacheKeySeparator)
	builder.WriteString(strconv.FormatUint(p.Seq(), 10))
	return builder.String()
}

// PutPkt put a Pkt into cache.
// If all Pkt of a msg payload collected,
// a FullPktResult contains sender id string and all Pkt list will be push to fullPktNotifyC.
func (c *Cache) PutPkt(sender string, pkt *Pkt) bool {
	c.mu.RLock()
	if pkt.pktTotal == 1 && pkt.pktSeq == 0 {
		// only one pkt
		c.mu.RUnlock()
		pktList := make([]*Pkt, 1)
		pktList[0] = pkt
		c.fullPktNotifyC <- &FullPktResult{
			Sender:  sender,
			PktList: pktList,
		}
		return true
	}
	c.mu.RUnlock()
	key := c.createKey(sender, pkt)

	c.mu.Lock()
	defer c.mu.Unlock()
	pktList, ok := c.pktMap[key]
	if ok {
		pktSeq := pkt.pktSeq
		pktTotal := len(pktList)
		if pktSeq >= uint8(pktTotal) {
			return false
		}
		if pktList[pktSeq] != nil {
			return false
		}
		pktList[pktSeq] = pkt
		stat := c.statMap[key]
		currentTotal := atomic.AddUint32(stat, 1)
		if currentTotal >= uint32(pktTotal) {
			delete(c.statMap, key)
			delete(c.pktMap, key)
			delete(c.receiveFirstTimeMap, key)
			c.fullPktNotifyC <- &FullPktResult{
				Sender:  sender,
				PktList: pktList,
			}
		}
		return true
	}
	pktTotal := pkt.pktTotal
	pktList = make([]*Pkt, pktTotal)
	pktList[pkt.pktSeq] = pkt
	stat := uint32(1)
	c.statMap[key] = &stat
	c.pktMap[key] = pktList
	c.receiveFirstTimeMap[key] = utils.CurrentTimeMillisSeconds()
	return true
}
