/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"errors"
	"io"
)

const bufferMaxLen = 1024 * 1024 * 1024

// msgBuffer
// @Description: buffer read struct ,allow peek,if not know peek,don't use it.
//               提供缓冲读的buffer对象，允许调用peek函数，如果看到buffer+peek这个组合只知道中文翻译意思，
//               但不知道这个函数的具体含义和内部逻辑，就不建议使用
//               这个对象不对外开放，为了代码简洁，只针对内部使用方式进行了代码编写
//               线程不安全
type msgBuffer struct {
	conn   io.Reader
	buffer []byte
}

//  newMsgBuffer .
//  @Description:
//  @param conn
//  @return *msgBuffer
func newMsgBuffer(conn io.Reader) *msgBuffer {
	return &msgBuffer{conn: conn, buffer: make([]byte, 0)}
}

//  peek .
//  @Description:preread without consuming them
//  @receiver m
//  @param b
//  @return int
//  @return error
func (m *msgBuffer) peek(b []byte) (int, error) {
	if len(b)+len(m.buffer) > bufferMaxLen {
		return 0, errors.New("peek too long")
	}
	if len(m.buffer) >= len(b) {
		copy(b, m.buffer[:len(b)])
		return len(b), nil
	}
	needRead := len(b) - len(m.buffer)
	bufferRead := make([]byte, needRead)
	read, err := io.ReadFull(m.conn, bufferRead)
	if err != nil {
		return read, err
	}
	m.buffer = append(m.buffer, bufferRead...)
	return copy(b, m.buffer), nil
}

//  Read .
//  @Description:Read and consume
//  @receiver m
//  @param b
//  @return int
//  @return error
func (m *msgBuffer) Read(b []byte) (int, error) {
	bufferLen := len(m.buffer)
	if bufferLen == 0 {
		return m.conn.Read(b)
	}
	if bufferLen >= len(b) {
		n := copy(b, m.buffer)
		m.buffer = m.buffer[n:]
		return n, nil
	}
	// len(b)>bufferLen && bufferLen!=0
	needRead := len(b) - bufferLen
	bufferRead := make([]byte, needRead)
	read, err := m.conn.Read(bufferRead)
	if err != nil {
		return read, err
	}
	n := copy(b, append(m.buffer, bufferRead...))
	m.buffer = m.buffer[:0]
	return n, nil
}
