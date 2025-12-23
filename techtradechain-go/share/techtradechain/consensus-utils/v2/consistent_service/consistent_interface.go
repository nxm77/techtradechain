/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package consistent_service is consistent service for consensus engine
package consistent_service

import (
	"context"
)

// ConsistentEngine 一致性引擎，用于节点间（共识状态）信息同步
type ConsistentEngine interface {

	// Start 启动一致性引擎
	// ctx 后续扩展用，启动失败返回error
	Start(ctx context.Context) error

	// Stop 停止一致性引擎
	// ctx 后续扩展用，，停止失败返回error
	Stop(ctx context.Context) error

	// AddBroadcaster 添加状态广播器（如一个tbft状态广播器）
	// id 广播器标识，需要保证唯一性
	// broadcast 广播器，需要用户自己实现（如：tbft广播器、maxbft广播器）
	AddBroadcaster(id string, broadcast StatusBroadcaster) error

	// UpdateNodeStatus 更新本地状态
	// id 节点标识，需要保证唯一性
	// node 需要更新的节点信息（包含节点状态，节点状态可以有多种）
	UpdateNodeStatus(id string, node Node) error

	// PutRemoter 添加节点
	// id 节点标识，需要保证唯一性
	// node 需要添加的节点信息（包含节点状态，节点状态可以有多种）
	PutRemoter(id string, node Node) error

	// RemoveRemoter 删除节点
	// id 节点标识，当节点不存在时返回错误消息
	RemoveRemoter(id string) error

	// RegisterStatusCoder 注册状态解析器
	// decoderType 解析器标识，需要保证唯一性
	// decoder 需要添加的解析器，由用户实现（如：tbft解析器）
	RegisterStatusCoder(decoderType int8, decoder Decoder) error

	// RegisterStatusInterceptor 注册过滤器
	// interceptorType 过滤器标识，需要保证唯一性
	// interceptor 需要添加的过滤器，由用户实现（如：tbft过滤器）
	RegisterStatusInterceptor(interceptorType int8, interceptor StatusInterceptor) error
}

// Message 用户一致性引擎于其他模块（如tbft/maxbft）数据交互
type Message interface {

	// Send 一致性引擎对外发送数据
	// payload 需要发送的消息
	Send(payload interface{})

	// Receive 一致性引擎接收外部数据
	// 返回接收到的消息
	Receive() interface{}

	// Start 启动
	Start() error

	// Stop 关闭
	Stop() error
}

// StatusBroadcaster 状态广播器
// 由内部定时器触发广播
// 根据LocalNodeStatus和RemoteNodeStatus当前状态，确认是否进行状态广播
type StatusBroadcaster interface {

	// ID 广播器标识
	ID() string

	// TimePattern 状态广播触发模式
	// 返回触发间隔
	TimePattern() interface{}

	// PreBroadcaster 广播器，判断是否要发送消息
	// 返回广播器方法
	PreBroadcaster() Broadcast

	// Start 启动
	Start() error

	// Stop 停止
	Stop() error

	// IsRunning 获取运行状态
	IsRunning() bool
}

// Broadcast 广播器
type Broadcast func(Node, Node) (interface{}, error)

// Node 节点信息（基本信息/共识状态）
type Node interface {

	// ID 节点标识
	ID() string

	// Statuses 节点状态，可以有多种
	Statuses() map[int8]Status

	// UpdateStatus 更新节点状态
	// status 节点状态（如：tbft状态）
	UpdateStatus(status Status)
}

// StatusInterceptor 过滤器
type StatusInterceptor interface {

	// Handle 过滤处理方法
	// status 节点状态（如：tbft状态）
	Handle(status Status) error
}

// Decoder 解析器
type Decoder interface {

	// MsgType 解析器处理的消息类型
	MsgType() int8

	// Decode 解析器解析对应类型的消息，返回解析后数据对象
	Decode(interface{}) interface{}
}

// Status 节点共识状态
type Status interface {

	// Type 状态类型
	Type() int8

	// Data 状态内容
	Data() interface{}

	// Update 更新状态
	Update(status Status)
}

// Logger is logger interface of consistentEngine.
type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Debugw(msg string, keysAndValues ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Warnw(msg string, keysAndValues ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Infow(msg string, keysAndValues ...interface{})
}
