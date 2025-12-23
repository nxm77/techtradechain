/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkt

import "errors"

var (
	// ErrEmptyPktList the error if the pkt list is empty
	ErrEmptyPktList = errors.New("empty pkt list")

	// ErrPktCountMismatch the error of the pkt count mismatch
	ErrPktCountMismatch = errors.New("pkt count mismatch")

	// ErrWrongSeq the error of the seq number is wrong
	ErrWrongSeq = errors.New("wrong seq number")

	// ErrWrongPktSeq he error of the pkt seq number is wrong
	ErrWrongPktSeq = errors.New("wrong pkt seq number")

	// BytesAssembler .
	BytesAssembler = &Assembler{}
)

// Assembler provides a function assemble some Pkt instances which disassembled to a slice of bytes.
type Assembler struct {
}

// AssembleBytes assembles some Pkt instances which disassembled to a slice of bytes.
func (a *Assembler) AssembleBytes(pktList []*Pkt) (bytes, protocol []byte, err error) {
	pktCount := len(pktList)
	if pktCount == 0 {
		return nil, nil, ErrEmptyPktList
	}
	var (
		seq      = pktList[0].seq
		pktTotal = pktList[0].pktTotal
	)

	if pktCount != int(pktTotal) {
		return nil, nil, ErrPktCountMismatch
	}
	avgLen := len(pktList[0].bytes)
	lastLen := len(pktList[pktCount-1].bytes)
	dataLength := (pktCount-1)*avgLen + lastLen
	bytes = make([]byte, dataLength)
	for i := 0; i < pktCount; i++ {
		pkt := pktList[i]
		if seq != pkt.seq {
			return nil, nil, ErrWrongSeq
		}
		if i != int(pkt.pktSeq) {
			return nil, nil, ErrWrongPktSeq
		}
		startIdx := i * avgLen
		copy(bytes[startIdx:], pkt.bytes)
		if i == pktCount-1 {
			protocol = make([]byte, pkt.protocolLen)
			copy(protocol[:], pkt.protocol[:])
		}
	}
	return
}
