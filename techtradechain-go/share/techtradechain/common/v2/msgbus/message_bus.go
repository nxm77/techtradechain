/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package msgbus

import (
	"fmt"
	"sync"
	"time"
)

const (
	defaultMessageBufferSize = 10240
)

var (
	topicCheckPeriod = 10 * time.Minute
)

// MessageBus provides a pub-sub interface for cross-module communication
type MessageBus interface {
	// A subscriber register on s specific topic.
	// When a message on this topic is published, the subscriber's OnMessage() is called.
	Register(topic Topic, sub Subscriber)
	// UnRegister unregister the subscriber from message bus.
	UnRegister(topic Topic, sub Subscriber)
	// Used to publish a message on this message bus to notify subscribers.
	Publish(topic Topic, payload interface{})
	// Used to publish a message on this message bus to notify subscribers.
	// Safe mode, make sure the subscriber's OnMessage() is called with the order of a message on this topic is published.
	PublishSafe(topic Topic, payload interface{})
	// Used to publish a message on this message bus to notify subscribers.
	// Sync mod, make sure all messages  are completed sequentially.
	PublishSync(topic Topic, payload interface{})
	// Close the message bus, all publishes are ignored.
	Close()
}

// Subscriber should implement these methods,
type Subscriber interface {
	// When a message with topic A is published on the message bus,
	// all the subscribers's OnMessage() methods of topic A are called.
	OnMessage(*Message)

	// When the message bus is shutting down,
	OnQuit()
}

type messageBusImpl struct {
	mu sync.RWMutex

	// topic -> subscriber list
	// topicMap map[Topic]*list.List
	topicMap          sync.Map
	activeTasks       sync.Map // 记录当前正在处理的 topic 及其开始时间
	once              sync.Once
	channelMap        sync.Map // 记录可无序处理的topic对应的channel
	inorderChannelMap sync.Map // 记录需要按序处理的topic对应的channel
	// messageC     chan *Message
	//safeMessageC chan *Message
	quitC  chan struct{}
	closed bool
}

// NewMessageBus 构造一个MessageBus对象
// @return MessageBus
func NewMessageBus() MessageBus {
	return &messageBusImpl{
		mu: sync.RWMutex{},
		// topicMap:     make(map[Topic]*list.List),
		topicMap:          sync.Map{},
		activeTasks:       sync.Map{},
		once:              sync.Once{},
		channelMap:        sync.Map{},
		inorderChannelMap: sync.Map{},
		// messageC:     make(chan *Message, defaultMessageBufferSize),
		//safeMessageC: make(chan *Message, defaultMessageBufferSize),
		quitC:  make(chan struct{}),
		closed: false,
	}
}

// Message 消息对象
type Message struct {
	Topic   Topic
	Payload interface{}
}

// Register topic for subscriber
func (b *messageBusImpl) Register(topic Topic, sub Subscriber) {
	b.once.Do(func() {
		b.handleMessageLooping()
		b.startLoggingLongRunningTopics()
	})

	// Lock to protect b.closed & slice of subscribers
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	r, _ := b.topicMap.Load(topic)
	if r == nil {
		// The topic has never been registered, make a new array to for subscribers
		b.topicMap.Store(topic, make([]Subscriber, 0))
		// make one channel for each topic without order
		msgC := make(chan *Message, defaultMessageBufferSize)
		b.channelMap.Store(topic, msgC)

		// make one channel for each topic in order
		inOrderMsgC := make(chan *Message, defaultMessageBufferSize)
		b.inorderChannelMap.Store(topic, inOrderMsgC)
		// start listening channel message for topic
		go func() {
			for {
				select {
				case <-b.quitC:
					return
				case m := <-msgC:
					go b.notify(m, false)
				}
			}
		}()

		// start listening channel message for topic in order
		go func() {
			for {
				select {
				case <-b.quitC:
					return
				case m := <-inOrderMsgC:
					b.notify(m, true)
				}
			}
		}()

	}
	subs, _ := b.topicMap.Load(topic)

	s, _ := subs.([]Subscriber)
	// If the same subscriber instance has exist, then return to avoid redundancy
	if isRedundant(s, sub) {
		return
	}
	s = append(s, sub)
	b.topicMap.Store(topic, s)
}

// UnRegister implements the MessageBus interface.
func (b *messageBusImpl) UnRegister(topic Topic, sub Subscriber) {
	// Lock to protect b.closed & slice of subscribers
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	if subscribers, ok := b.topicMap.Load(topic); ok {
		subs, _ := subscribers.([]Subscriber)
		for i, s := range subs {
			if s == sub {
				newSubs := append(subs[:i], subs[i+1:]...)
				b.topicMap.Store(topic, newSubs)
			}
		}
	}
}

// Publish 发布消息
// @param topic
// @param payload
func (b *messageBusImpl) Publish(topic Topic, payload interface{}) {
	b.publish(topic, payload, false)
}

// PublishSafe 发布消息
// @param topic
// @param payload
func (b *messageBusImpl) PublishSafe(topic Topic, payload interface{}) {
	b.publish(topic, payload, true)
}

func (b *messageBusImpl) publish(topic Topic, payload interface{}, safe bool) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return
	}

	// fetch the channel for topic & push message into channel
	var c interface{}
	if safe {
		c, _ = b.inorderChannelMap.Load(topic)
	} else {
		c, _ = b.channelMap.Load(topic)
	}
	// 防止由于下面堵塞导致锁无法释放
	b.mu.RUnlock()
	if c == nil {
		//fmt.Printf("WARN: topic %s is not registered", topic.String()) // stop logging to console
		return
	}

	channel, _ := c.(chan *Message)
	channel <- &Message{Topic: topic, Payload: payload}
}

// PublishSync 同步发送消息
// @param topic
// @param payload
func (b *messageBusImpl) PublishSync(topic Topic, payload interface{}) {
	val, _ := b.topicMap.Load(topic)
	if val == nil {
		return
	}
	subs, _ := val.([]Subscriber)

	for _, sub := range subs {
		sub.OnMessage(&Message{topic, payload})
	}
}

// Close 关闭消息通道
func (b *messageBusImpl) Close() {
	// support multiple Close()
	select {
	case <-b.quitC:
	default:
		close(b.quitC)
		// 关闭普通channel map
		b.channelMap.Range(func(_, v interface{}) bool {
			channel, _ := v.(chan *Message)
			close(channel)
			return true
		})

		// 关闭有序channel map
		b.inorderChannelMap.Range(func(_, v interface{}) bool {
			channel, _ := v.(chan *Message)
			close(channel)
			return true
		})

		// close(b.messageC)
		//close(b.safeMessageC)
	}
}

func (b *messageBusImpl) handleMessageLooping() {
	go func() {
		for range b.quitC {
			b.mu.Lock()
			// for each top, notify subscribes that message bus is quiting now
			b.topicMap.Range(func(_, v interface{}) bool {
				s, _ := v.([]Subscriber)
				length := len(s)
				for i := 0; i < length; i++ {
					s := s[i]
					go s.OnQuit()
				}
				return true
			})
			b.closed = true
			b.mu.Unlock()
			return
		}
	}()
}

type activeTopicTask struct {
	startTime time.Time
	sub       Subscriber
}

// notify subscribers when msg comes
func (b *messageBusImpl) notify(m *Message, isSafe bool) {
	if m.Topic <= 0 {
		return
	}

	// Remove topic when processing is done
	defer b.activeTasks.Delete(m.Topic)

	// fetch the subscribers for topic
	val, _ := b.topicMap.Load(m.Topic)
	if val == nil {
		return
	}
	subs, _ := val.([]Subscriber)
	for _, sub := range subs {
		if isSafe {
			// Mark the topic as active with start time
			b.activeTasks.Store(m.Topic, activeTopicTask{
				startTime: time.Now(),
				sub:       sub,
			})

			sub.OnMessage(m) // notify in order
		} else {
			go sub.OnMessage(m)
		}
	}
}

// Start a background task to check for long-running topics
func (b *messageBusImpl) startLoggingLongRunningTopics() {
	go func() {
		ticker := time.NewTicker(topicCheckPeriod) // Check every 10 minutes
		defer ticker.Stop()

		for range ticker.C {
			currentTime := time.Now()
			b.activeTasks.Range(func(key, value interface{}) bool {
				topic, ok := key.(Topic)
				if !ok {
					fmt.Printf("key is not of type Topic, actual type: %T \n", key)
					return false
				}

				task, ok := value.(activeTopicTask)
				if !ok {
					fmt.Printf("value is not of type activeTopicTask, actual type: %T", value)
					return false
				}

				// If processing time exceeds 10 minutes, log the topic and module
				if currentTime.Sub(task.startTime) > topicCheckPeriod {
					fmt.Printf("[%s] Long-running topic detected: Topic=%d, Module:%T, Duration=%dms\n",
						time.Now().String(), topic, task.sub, currentTime.Sub(task.startTime).Milliseconds())
				}
				return true
			})
		}
	}()
}

func isRedundant(subs []Subscriber, sub Subscriber) bool {
	for _, s := range subs {
		if s == sub {
			return true
		}
	}
	return false
}

// DefaultSubscriber 默认消息订阅者
type DefaultSubscriber struct{}

// OnMessage 接收广播的消息进行处理
// @param *Message
func (DefaultSubscriber) OnMessage(*Message) {
	// just for mock
}

// OnQuit 关闭消息通道
func (DefaultSubscriber) OnQuit() {
	// just for mock
}
