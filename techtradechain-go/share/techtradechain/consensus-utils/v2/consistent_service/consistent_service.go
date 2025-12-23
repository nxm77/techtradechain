/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package consistent_service

import (
	"context"
	"sync"
	"time"

	netpb "techtradechain.com/techtradechain/pb-go/v2/net"
)

// StatusConsistentEngine 一致性引擎实现
type StatusConsistentEngine struct {
	// lock read write lock
	sync.RWMutex
	// log  [Consistent]
	log Logger
	//状态广播器
	broadcasters map[string]StatusBroadcaster
	//本地节点信息
	local Node
	//远端节点信息
	remoters map[string]Node
	//解析器
	decoders map[int8]Decoder
	//拦截器
	interceptors map[int8]StatusInterceptor
	//发送/接收消息
	msg Message
	//运行状态
	running bool
}

// NewConsistentService nolint: unused
func NewConsistentService(local Node,
	msg Message,
	log Logger) *StatusConsistentEngine {
	bcs := StatusConsistentEngine{
		broadcasters: make(map[string]StatusBroadcaster),
		remoters:     make(map[string]Node),
		decoders:     make(map[int8]Decoder),
		interceptors: make(map[int8]StatusInterceptor),
		log:          log,
		local:        local,
		msg:          msg,
		running:      false,
	}
	log.Debugf("local id: %s", local.ID())

	return &bcs
}

// AddBroadcaster add a broadcaster
func (e *StatusConsistentEngine) AddBroadcaster(id string, broadcaster StatusBroadcaster) error {

	if id == "" || broadcaster == nil || id != broadcaster.ID() {
		return ErrorInvalidParameter
	}
	if e.broadcasters[id] != nil {
		return ErrorBroadcasterExist
	}
	e.broadcasters[id] = broadcaster
	e.log.Infof("AddBroadcaster(%s) succeed", id)

	return nil
}

// UpdateNodeStatus 更新本地状态
func (e *StatusConsistentEngine) UpdateNodeStatus(id string, node Node) error {
	if id == "" || node == nil {
		return ErrorInvalidParameter
	}

	e.log.Infof("UpdateNodeStatus:%s", id)
	// 拦截器
	for _, interceptor := range e.interceptors {
		for _, status := range node.Statuses() {
			err := interceptor.Handle(status)
			if err != nil {
				return err
			}
		}

	}

	e.Lock()
	defer e.Unlock()
	// 更新节点状态
	if id == e.local.ID() {
		e.local = node

	} else {
		if e.remoters[id] != nil {
			e.remoters[id] = node
		}
	}
	return nil
}

// PutRemoter put a remoter
func (e *StatusConsistentEngine) PutRemoter(id string, node Node) error {
	if id == "" || node == nil {
		return ErrorInvalidParameter
	}

	e.Lock()
	defer e.Unlock()
	e.log.Infof("AddRemoter:%s", id)

	if e.local.ID() == id {
		return ErrorRemoterEqualLocal
	}
	if e.remoters[id] != nil {
		return ErrorRemoterExist
	}
	e.remoters[id] = node
	e.log.Debugf("after AddRemoter,remoters size is %d", len(e.remoters))

	return nil
}

// RemoveRemoter remove remoter
func (e *StatusConsistentEngine) RemoveRemoter(id string) error {
	if id == "" {
		return ErrorInvalidParameter
	}

	e.Lock()
	defer e.Unlock()
	e.log.Infof("DelRemoter:%s", id)
	if e.remoters[id] == nil {
		e.log.Warnf("DelRemoter: %s is not exist", id)
		return ErrorRemoterNotExist
	}

	delete(e.remoters, id)

	e.log.Debugf("after DelRemoter,remoters size is %d", len(e.remoters))

	return nil
}

// RegisterStatusCoder register status coder
func (e *StatusConsistentEngine) RegisterStatusCoder(decoderType int8, decoder Decoder) error {
	if decoder == nil || decoderType != decoder.MsgType() {
		return ErrorInvalidParameter
	}

	e.log.Info("RegisterStatusCoder:", decoderType)
	if e.decoders[decoderType] != nil {
		//已经存在
		e.log.Warnf("RegisterStatusCoder(%d) is exist", decoderType)
		return ErrorDecoderExist
	}
	e.decoders[decoderType] = decoder

	return nil
}

// RegisterStatusInterceptor register status interceptor
func (e *StatusConsistentEngine) RegisterStatusInterceptor(interceptorType int8, interceptor StatusInterceptor) error {
	if interceptor == nil {
		return ErrorInvalidParameter
	}

	if e.interceptors[interceptorType] != nil {
		return ErrorInterceptorExist
	}
	e.log.Infof("RegisterStatusInterceptor, size before:%d", len(e.interceptors))
	e.interceptors[interceptorType] = interceptor

	return nil
}

func (e *StatusConsistentEngine) handleReceiveMessage() {
	e.log.Debugf("start handleReceiveMessage")
	defer e.log.Debugf("exit handleReceiveMessage")
	for {
		if !e.running {
			return
		}
		m := e.msg.Receive()
		if m == nil {
			e.log.Debugf("receive nil")
			continue
		}
		e.log.Debugf("receive payload")
		//遍历已经注册的解析器，使用解析器处理收到的消息，更新节点状态
		for _, v := range e.decoders {
			e.log.Debugf("decoders MsgType is %d", v.MsgType())

			data := v.Decode(m)
			rs, ok := data.(Node)
			if !ok {
				rs, ok := data.(string)
				if ok {
					e.log.Debugf("decoders.Decode return %s", rs)
				}
				continue
			}

			e.RLock()
			remoteInfo := e.remoters[rs.ID()]
			e.RUnlock()
			if remoteInfo == nil {
				e.log.Debugf("receive message not from expected node: %s", rs.ID())
				continue
			}

			e.log.Debugf("update status, remoter id is %s", rs.ID())
			for _, v := range rs.Statuses() {
				e.log.Debugf("status type:%d", v.Type())
				remoteInfo.UpdateStatus(v)
			}

		}
	}
}

// Start engine
func (e *StatusConsistentEngine) Start(ctx context.Context) error {
	if e.running {
		return ErrorRunRepeatedly
	}
	if ctx != nil && ctx.Err() != nil {
		e.log.Debugf(ctx.Err().Error())
	}

	err := e.msg.Start()
	if err != nil {
		return err
	}

	// 设置一致性引擎为运行状态
	e.running = true

	// 处理接收到的消息
	go e.handleReceiveMessage()
	// 通过ticker方式，间隔一定时间，广播消息
	err = e.handle()
	if err != nil {
		return err
	}

	return nil
}

func (e *StatusConsistentEngine) tickerHandle(
	broadcaster StatusBroadcaster, interval interface{}) {
	t, ok := interval.(time.Duration)
	if !ok {
		return
	}
	ticker := time.NewTicker(t)
	defer ticker.Stop()
	// 每间隔一定时间，通过PreBroadcaster()函数判断是否需要给其他节点发送消息
	for range ticker.C {
		if !broadcaster.IsRunning() {
			break
		}
		preBroadcast := broadcaster.PreBroadcaster()
		// 遍历远端节点列表
		remotes := make(map[string]Node)
		e.RLock()
		for id, remoter := range e.remoters {
			remotes[id] = remoter
		}
		e.RUnlock()
		for _, v := range remotes {
			netMSGs, err := preBroadcast(e.local, v)
			if err != nil {
				e.log.Errorf("preBroadcast err:%s", err)
				continue
			}

			nms, ok := netMSGs.([]*netpb.NetMsg)
			if !ok {
				e.log.Errorf("netMSGs is't []*netpb.NetMsg")
				continue
			}
			for n := 0; n < len(nms); n++ {
				e.msg.Send(nms[n])
			}
		}
	}
}

func (e *StatusConsistentEngine) handle() error {
	e.log.Infof("StatusConsistentEngine Start")

	for _, broadcaster := range e.broadcasters {
		if !broadcaster.IsRunning() {
			e.log.Infof("broadcasters[%s] Start", broadcaster.ID())

			err := broadcaster.Start()
			if err != nil {
				return err
			}

			go e.tickerHandle(broadcaster, broadcaster.TimePattern())
		}
	}

	return nil
}

// Stop engine
func (e *StatusConsistentEngine) Stop(ctx context.Context) error {
	if !e.running {
		return ErrorNotRunning
	}
	if ctx != nil && ctx.Err() != nil {
		e.log.Debugf(ctx.Err().Error())
	}
	e.log.Infof("StatusConsistentEngine Close")

	for _, v := range e.broadcasters {
		if v.IsRunning() {
			err := v.Stop()
			if err != nil {
				return err
			}
		}
	}
	e.running = false
	err := e.msg.Stop()
	if err != nil {
		return err
	}

	return nil
}
