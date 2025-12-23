package rpc

import (
	"fmt"
	"io"
	"sync"

	cmap "github.com/orcaman/concurrent-map"

	"go.uber.org/atomic"

	protocol "techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-engine/v2/pb/protogo"
	"techtradechain.com/techtradechain/vm-engine/v2/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	runtimeServiceOnce     sync.Once
	runtimeServiceInstance *RuntimeService
)

// RuntimeService is the sandbox - techtradechain service
type RuntimeService struct {
	streamCounter    atomic.Uint64
	lock             sync.RWMutex
	logger           protocol.Logger
	sandboxMsgNotify cmap.ConcurrentMap
	//stream           protogo.DockerVMRpc_DockerVMCommunicateServer
	//sandboxMsgNotify map[string]func(msg *protogo.DockerVMMessage, sendMsg func(msg *protogo.DockerVMMessage))
	//responseChanMap  map[uint64]chan *protogo.DockerVMMessage
	//responseChanMap  sync.Map
}

// NewRuntimeService returns runtime service
func NewRuntimeService(logger protocol.Logger) *RuntimeService {
	cmap.SHARD_COUNT = 1024
	runtimeServiceOnce.Do(func() {
		runtimeServiceInstance = &RuntimeService{
			streamCounter:    atomic.Uint64{},
			lock:             sync.RWMutex{},
			logger:           logger,
			sandboxMsgNotify: cmap.New(),
			//responseChanMap:  sync.Map{},
		}
	})
	return runtimeServiceInstance
}

func (s *RuntimeService) getStreamId() uint64 {
	return s.streamCounter.Add(1)
}

//func (s *RuntimeService) registerStreamSendCh(streamId uint64, sendCh chan *protogo.DockerVMMessage) bool {
//	s.logger.Debugf("register send chan for stream[%d]", streamId)
//	if _, ok := s.responseChanMap.Load(streamId); ok {
//		s.logger.Debugf("[%d] fail to register receive chan cause chan already registered", streamId)
//		return false
//	}
//	s.responseChanMap.Store(streamId, sendCh)
//	return true
//}

// nolint: unused
//func (s *RuntimeService) getStreamSendCh(streamId uint64) chan *protogo.DockerVMMessage {
//	s.logger.Debugf("get send chan for stream[%d]", streamId)
//	ch, ok := s.responseChanMap.Load(streamId)
//	if !ok {
//		return nil
//	}
//
//	return ch.(chan *protogo.DockerVMMessage)
//}

//func (s *RuntimeService) deleteStreamSendCh(streamId uint64) {
//	s.logger.Debugf("delete send chan for stream[%d]", streamId)
//	s.responseChanMap.Delete(streamId)
//}

type serviceStream struct {
	logger         protocol.Logger
	streamId       uint64
	stream         protogo.DockerVMRpc_DockerVMCommunicateServer
	sendResponseCh chan *protogo.DockerVMMessage
	stopSend       chan struct{}
	stopReceive    chan struct{}
	wg             *sync.WaitGroup
}

func (ss *serviceStream) putResp(msg *protogo.DockerVMMessage) {
	ss.logger.DebugDynamic(func() string {
		return fmt.Sprintf("put sys_call response to send chan, txId [%s], type [%s]", msg.TxId, msg.Type)
	})
	ss.sendResponseCh <- msg

}

// DockerVMCommunicate is the runtime docker vm communicate stream
func (s *RuntimeService) DockerVMCommunicate(stream protogo.DockerVMRpc_DockerVMCommunicateServer) error {
	ss := &serviceStream{
		logger:         s.logger,
		streamId:       s.getStreamId(),
		stream:         stream,
		sendResponseCh: make(chan *protogo.DockerVMMessage, 1),
		stopSend:       make(chan struct{}, 1),
		stopReceive:    make(chan struct{}, 1),
		wg:             &sync.WaitGroup{},
	}
	//defer s.deleteStreamSendCh(ss.streamId)

	//s.registerStreamSendCh(ss.streamId, ss.sendResponseCh)

	ss.wg.Add(2)

	go s.recvRoutine(ss)
	go s.sendRoutine(ss)

	ss.wg.Wait()
	return nil
}

func (s *RuntimeService) recvRoutine(ss *serviceStream) {

	s.logger.Debugf("start receiving sandbox message")

	for {
		select {
		case <-ss.stopReceive:
			s.logger.Debugf("stop runtime server receive goroutine")
			ss.wg.Done()
			return
		default:
			msg := utils.DockerVMMessageFromPool()
			err := ss.stream.RecvMsg(msg)
			if err != nil {
				if err == io.EOF || status.Code(err) == codes.Canceled {
					s.logger.Debugf("sandbox client grpc stream closed (context cancelled)")
				} else {
					s.logger.Errorf("runtime server receive error %s", err)
				}
				close(ss.stopSend)
				ss.wg.Done()
				return
			}

			s.logger.DebugDynamic(func() string {
				return fmt.Sprintf("runtime server recveive msg, txId [%s], type [%s]", msg.TxId, msg.Type)
			})
			switch msg.Type {
			case protogo.DockerVMType_TX_RESPONSE,
				protogo.DockerVMType_CALL_CONTRACT_REQUEST,
				protogo.DockerVMType_GET_STATE_REQUEST,
				protogo.DockerVMType_GET_BATCH_STATE_REQUEST,
				protogo.DockerVMType_CREATE_KV_ITERATOR_REQUEST,
				protogo.DockerVMType_CONSUME_KV_ITERATOR_REQUEST,
				protogo.DockerVMType_CREATE_KEY_HISTORY_ITER_REQUEST,
				protogo.DockerVMType_CONSUME_KEY_HISTORY_ITER_REQUEST,
				protogo.DockerVMType_GET_SENDER_ADDRESS_REQUEST:

				//if msg.Type == protogo.DockerVMType_TX_RESPONSE {
				//	utils.EnterNextStep(msg, protogo.StepType_RUNTIME_GRPC_RECEIVE_TX_RESPONSE, "")
				//}

				notify := s.getNotify(msg.ChainId, msg.TxId)

				if notify == nil {
					s.logger.DebugDynamic(func() string {
						return fmt.Sprintf("get receive notify[%s] failed, please check your key", msg.TxId)
					})
					break
				}
				notify(msg, ss.putResp)
			}
		}
	}

}

func (s *RuntimeService) sendRoutine(ss *serviceStream) {
	s.logger.Debugf("start sending sys_call response")
	for {
		select {
		case msg := <-ss.sendResponseCh:
			s.logger.DebugDynamic(func() string {
				return fmt.Sprintf("get sys_call response from send chan, send to sandbox, "+
					"txId [%s], type [%s]", msg.TxId, msg.Type)
			})
			if err := ss.stream.Send(msg); err != nil {
				errStatus, _ := status.FromError(err)
				s.logger.Errorf("fail to send msg: err: %s, err message: %s, err code: %s",
					err, errStatus.Message(), errStatus.Code())
				if errStatus.Code() != codes.ResourceExhausted {
					close(ss.stopReceive)
					ss.wg.Done()
					return
				}
			}
		case <-ss.stopSend:
			ss.wg.Done()
			s.logger.Debugf("stop runtime server send goroutine")
			return
		}
	}
}

// RegisterSandboxMsgNotify register sandbox msg notify
func (s *RuntimeService) RegisterSandboxMsgNotify(chainId, txKey string,
	respNotify func(msg *protogo.DockerVMMessage, sendF func(*protogo.DockerVMMessage))) error {
	notifyKey := utils.ConstructNotifyMapKey(chainId, txKey)
	s.logger.DebugDynamic(func() string {
		return fmt.Sprintf("register receive respNotify for [%s]", notifyKey)
	})
	if _, ok := s.sandboxMsgNotify.Get(notifyKey); ok {
		s.logger.Errorf("[%s] fail to register respNotify cause ")
	}
	s.sandboxMsgNotify.Set(notifyKey, respNotify)
	return nil
}

func (s *RuntimeService) getNotify(chainId, txId string) func(msg *protogo.DockerVMMessage,
	f func(msg *protogo.DockerVMMessage)) {
	notifyKey := utils.ConstructNotifyMapKey(chainId, txId)
	s.logger.DebugDynamic(func() string {
		return fmt.Sprintf("get notify for [%s]", notifyKey)
	})
	if notify, ok := s.sandboxMsgNotify.Get(notifyKey); ok {
		var notifyFunc func(msg *protogo.DockerVMMessage, f func(msg *protogo.DockerVMMessage))
		notifyFunc, ok = notify.(func(msg *protogo.DockerVMMessage, f func(msg *protogo.DockerVMMessage)))
		if ok {
			return notifyFunc
		}
	}
	return nil
}

// DeleteSandboxMsgNotify delete sandbox msg notify
func (s *RuntimeService) DeleteSandboxMsgNotify(chainId, txId string) bool {
	notifyKey := utils.ConstructNotifyMapKey(chainId, txId)
	s.logger.DebugDynamic(func() string {
		return fmt.Sprintf("[%s] delete notify", txId)
	})
	if _, ok := s.sandboxMsgNotify.Get(notifyKey); !ok {
		s.logger.Debugf("[%s] delete notify fail, notify is already deleted", notifyKey)
		return false
	}
	s.sandboxMsgNotify.Remove(notifyKey)
	return true
}
