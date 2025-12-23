/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkt

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var (
	startSeq                = uint64(time.Now().UnixNano() / 1000000 * 100000)
	defaultMinPktLen uint64 = 1 << 10 // 1M

	// BytesDisassembler is a global disassembler for disassembling bytes slice.
	BytesDisassembler = &Disassembler{minPktLen: defaultMinPktLen}

	// ErrProtocolTooLang the error of the protocol id is too lang
	ErrProtocolTooLang = errors.New("protocol too lang")
)

// SetMinPktLen will set the value of min pkt length for global bytes disassembler.
func SetMinPktLen(minPktLen uint64) {
	atomic.StoreUint64(&BytesDisassembler.minPktLen, minPktLen)
}

// Disassembler provides a function that can disassemble a slice of bytes to some Pkt instances.
type Disassembler struct {
	minPktLen uint64
}

// DisassembleBytes disassembles bytes slice to some Pkt instances.
// The length of protocol given must not be greater than 255.
// If success, The max size of Pkt list will not be greater than 256.
func (d *Disassembler) DisassembleBytes(bytes []byte, protocol []byte) ([]*Pkt, error) {
	// protocol length
	pl := len(protocol)
	if pl > 255 {
		return nil, ErrProtocolTooLang
	}
	// data length
	l := len(bytes)
	// get total packets count
	pktCount := d.getTotalPktCount(l)
	// get avg packet length and last packet length
	pktLengthAvg, pktLengthLast := d.getPktLength(pktCount, l)
	// make result slice
	res := make([]*Pkt, pktCount)
	pktCountInt := int(pktCount)
	wg := sync.WaitGroup{}
	wg.Add(pktCountInt)
	// get msg sequence
	seq := nextSeq()
	// disassemble with multi goroutines
	for i := 0; i < pktCountInt; i++ {
		go func(i int) {
			defer wg.Done()
			var startIdx, endIdx int
			// get start idx
			startIdx = i * pktLengthAvg
			pkt := &Pkt{
				seq:      seq,
				pktTotal: pktCount,
				pktSeq:   uint8(i),
				bytes:    nil,
			}
			if i < pktCountInt-1 {
				pkt.bytes = make([]byte, pktLengthAvg)
				endIdx = startIdx + pktLengthAvg
				copy(pkt.bytes, bytes[startIdx:endIdx])
			} else {
				pkt.protocolLen = uint8(pl)
				pkt.protocol = make([]byte, pl)
				copy(pkt.protocol, protocol[:])
				pkt.bytes = make([]byte, pktLengthLast)
				copy(pkt.bytes, bytes[startIdx:])
			}
			res[i] = pkt
		}(i)
	}
	wg.Wait()
	return res, nil
}

func (d *Disassembler) getTotalPktCount(bytesLen int) uint8 {
	if uint64(bytesLen) <= d.minPktLen {
		return 1
	}
	res := 0
	res = int(math.Ceil(float64(bytesLen) / float64(d.minPktLen)))
	if res > 255 {
		return 255
	}
	return uint8(res)
}

func (d *Disassembler) getPktLength(pkgCount uint8, bytesLen int) (int, int) {
	if pkgCount == 1 {
		return bytesLen, bytesLen
	}
	var avgLen, lastLen, pkgCountInt int
	pkgCountInt = int(pkgCount)
	avgLen = bytesLen / pkgCountInt
	lastLen = bytesLen - (pkgCountInt-1)*avgLen
	if lastLen > pkgCountInt {
		avgLen++
		lastLen = bytesLen - (pkgCountInt-1)*avgLen
	}
	return avgLen, lastLen
}

func nextSeq() uint64 {
	return atomic.AddUint64(&startSeq, 1)
}
