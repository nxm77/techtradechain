/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"bytes"
	"errors"
	"io"
	"unsafe"
)

type header struct {
	magic     [4]byte
	version   uint32
	msgLen    uint32
	extension [8]byte
}

const handShakeHeaderLen = 4

var handShakeHeader = [handShakeHeaderLen]byte{'h', 's', 'h', 'd'}

const msgVersion = 0

//  verifyHandShakeHead .
//  @Description: heck head
//  @param head
//  @return bool
func verifyHandShakeHead(head []byte) bool {
	return string(handShakeHeader[:]) == string(head[:4])
}

//  writeMsg .
//  @Description:
//  @param msg
//  @param conn
//  @return error
func writeMsg(msg []byte, conn io.Writer) error {
	var err error
	msgLen := len(msg)
	h1 := header{
		magic:     handShakeHeader,
		version:   msgVersion,
		msgLen:    uint32(msgLen),
		extension: [8]byte{},
	}
	// 内存对齐
	rawHeader := (*[unsafe.Sizeof(h1)]byte)(unsafe.Pointer(&h1))
	bytesBuffer := bytes.NewBuffer(rawHeader[:])
	bytesBuffer.Write(msg)
	length := len(bytesBuffer.Bytes())
	for start, n := 0, 0; start < length; {
		n, err = conn.Write(bytesBuffer.Bytes()[start:])
		if err != nil {
			return err
		}
		start += n
	}
	return nil
}

//  readMsg .
//  @Description: read a complete msg
//  @param conn
//  @return []byte
//  @return error
func readMsg(conn io.Reader) ([]byte, error) {
	rawHeader := [unsafe.Sizeof(header{})]byte{}
	_, err := io.ReadFull(conn, rawHeader[:])
	if err != nil {
		return nil, err
	}
	h1 := (*header)(unsafe.Pointer(&rawHeader))
	if !verifyHandShakeHead(h1.magic[:]) {
		return nil, errors.New("wrong header" + string(rawHeader[:]))
	}
	msgData := make([]byte, h1.msgLen)
	_, err = io.ReadFull(conn, msgData)
	if err != nil {
		return nil, err
	}
	return msgData, nil
}
