/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"net"
)

// msgBufferConn
// @Description: buffer read conn ,allow peek,if not know peek,don't use it.
//               提供缓冲读的conn对象，允许调用peek函数，如果看到buffer+peek这个组合只知道中文翻译意思，
//               但不知道这个函数的具体含义和内部逻辑，就不建议使用
//               这个对象不对外开放，为了代码简洁，只针对内部使用方式进行了代码编写
//               线程不安全
type msgBufferConn struct {
	net.Conn
	buffer *msgBuffer
}

//  newMsgBufferConn .
//  @Description:
//  @param c
//  @return *msgBufferConn
func newMsgBufferConn(c net.Conn) *msgBufferConn {
	return &msgBufferConn{
		Conn:   c,
		buffer: newMsgBuffer(c),
	}
}

//  Read .
//  @Description:
//  @receiver m
//  @param b
//  @return n
//  @return err
func (m *msgBufferConn) Read(b []byte) (n int, err error) {
	return m.buffer.Read(b)
}

// Peek .
//  @Description: preread without consuming them
//  @receiver m
//  @param b
//  @return n
//  @return err
func (m *msgBufferConn) Peek(b []byte) (n int, err error) {
	return m.buffer.peek(b)
}
