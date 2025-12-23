/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkt

import (
	"techtradechain.com/techtradechain/net-common/utils"

	"errors"
)

// Pkt is a unit packet for bytes assembler/disassembler.
// A slice of bytes can be disassembled to a number of Pkt.
// A number of Pkt can be assembled to a slice of bytes.
type Pkt struct {
	seq         uint64 // 8 bytes
	pktTotal    uint8  // 1 byte
	pktSeq      uint8  // 1 byte
	protocolLen uint8  // 1 byte
	protocol    []byte
	bytes       []byte
}

// Marshal a Pkt to a slice of bytes.
func (p *Pkt) Marshal() []byte {
	if p.protocolLen > 0 {
		l := 11 + int(p.protocolLen) + len(p.bytes)
		res := make([]byte, l)
		seqBytes := utils.Uint64ToBytes(p.seq)
		copy(res[:8], seqBytes)
		res[8] = p.pktTotal
		res[9] = p.pktSeq
		res[10] = p.protocolLen
		copy(res[11:11+p.protocolLen], p.protocol)
		copy(res[11+p.protocolLen:], p.bytes)
		return res
	}
	l := 11 + len(p.bytes)
	res := make([]byte, l)
	seqBytes := utils.Uint64ToBytes(p.seq)
	copy(res[:8], seqBytes)
	res[8] = p.pktTotal
	res[9] = p.pktSeq
	res[10] = byte(0)
	copy(res[11:], p.bytes)
	return res
}

// Unmarshal a slice of bytes to a Pkt.
func (p *Pkt) Unmarshal(payload []byte) error {
	payloadLen := len(payload)
	if payloadLen < 11 {
		return errors.New("payload too short")
	}
	seqBytes := make([]byte, 8)
	copy(seqBytes, payload[:8])
	p.seq = utils.BytesToUint64(seqBytes)
	p.pktTotal = payload[8]
	p.pktSeq = payload[9]
	p.protocolLen = payload[10]
	if p.protocolLen > 0 {
		p.protocol = make([]byte, p.protocolLen)
		protocolEndIdx := 11 + int(p.protocolLen)
		copy(p.protocol[:], payload[11:protocolEndIdx])
		bytesLen := payloadLen - protocolEndIdx
		p.bytes = make([]byte, bytesLen)
		copy(p.bytes[:], payload[protocolEndIdx:])
		return nil
	}
	bytesLen := payloadLen - 11
	p.bytes = make([]byte, bytesLen)
	copy(p.bytes[:], payload[11:])
	return nil
}

// Seq of the msg that Pkt belong to.
func (p *Pkt) Seq() uint64 {
	return p.seq
}
