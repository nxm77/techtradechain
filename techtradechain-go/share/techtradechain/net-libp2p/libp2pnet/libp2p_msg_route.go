package libp2pnet

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync"

	"techtradechain.com/techtradechain/common/v2/random/uuid"
	api "techtradechain.com/techtradechain/protocol/v2"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	TOPIC_MSG_ROUTER    = "msgRouter"
	ENV_ROUTER_MAX_STEP = "ROUTER_MAX_STEP"
)

type RouteMsg struct {
	From      peer.ID
	Middleman map[string]struct{}
	To        peer.ID
	MsgData   []byte
	MsgFlag   string
	MsgId     string
}

// newRouterController
// @Description: 初始化一个消息路由模块
// @param log
// @param ln
// @return ${return_type}
func newRouterController(log api.Logger, ln *LibP2pNet) *RouterController {
	return &RouterController{log: log, ln: ln, maxStepCount: -1}
}

// RouterController
// @Description:
type RouterController struct {
	localPeer    string
	ln           *LibP2pNet
	msgFilter    sync.Map
	msgArr       []string
	log          api.Logger
	maxStepCount int
}

// startMsgRouter
// @Description: 根据chainId,注册一个消息处理函数
// @receiver r
// @param chainId
// @return ${return_type}
func (r *RouterController) startMsgRouter(chainId string) {
	ln := r.ln
	log := r.log
	strRouterMaxStep := os.Getenv(ENV_ROUTER_MAX_STEP)
	nRouterMaxStep, err := strconv.Atoi(strRouterMaxStep)
	if err != nil {
		r.maxStepCount = -1
		log.Warnf("env %s value %s err %v", ENV_ROUTER_MAX_STEP, strRouterMaxStep, err)
	}
	if err == nil && nRouterMaxStep > 0 {
		r.maxStepCount = nRouterMaxStep
	}
	r.localPeer = ln.libP2pHost.host.ID().Pretty()
	if len(r.localPeer) == 0 {
		panic("localPeer is empty")
	}
	log.Infof("startMsgRouter chainId(%v) maxStepCount(%v)", chainId, r.maxStepCount)
	err = ln.DirectMsgHandle(chainId, TOPIC_MSG_ROUTER, func(sender string, msgData []byte) error {
		if r.maxStepCount <= 0 {
			return nil
		}
		localPeerId := r.localPeer
		msg := &RouteMsg{Middleman: map[string]struct{}{}}
		err := json.Unmarshal(msgData, msg)
		if err != nil {
			log.Error("json err:", err)
			return nil
		}
		if localPeerId == msg.To.Pretty() {
			log.Debugf("handler %v", string(msgData))
			handler := ln.messageHandlerDistributor.handler(chainId, msg.MsgFlag)
			if handler == nil {
				log.Warnf("[Net] handler not registered. drop message. (chainId:%s, flag:%s)", chainId, msg.MsgFlag)
				return nil
			}
			readMsgCallHandler(msg.From.Pretty(), msg.MsgData, handler, log)
			return nil
		}

		err = ln.router.msgRouter(chainId, msg)
		if err != nil {
			log.Debug("msg Router err:", err)
			return nil
		}
		return nil
	})
	if err != nil {
		log.Error("startMsgRouter chain(%v)err(%v)", chainId, err)
		return
	}

}

// stopMsgRouter
// @Description:
// @receiver r
// @param chainId
// @return ${return_type}
func (r *RouterController) stopMsgRouter(chainId string) {
	_ = r.ln.CancelDirectMsgHandle(chainId, TOPIC_MSG_ROUTER)
}

// sendByMsgRouter
// @Description: 消息路由转发，通过其他节点，将消息路由到目的节点
// @receiver r
// @param chainId
// @param pid
// @param msgFlag
// @param data
// @return ${return_type}
func (r *RouterController) sendByMsgRouter(chainId string, pid peer.ID, msgFlag string, data []byte) error {
	msg := &RouteMsg{
		From:      r.ln.libP2pHost.host.ID(),
		To:        pid,
		MsgFlag:   msgFlag,
		MsgData:   data,
		MsgId:     uuid.GetUUID(),
		Middleman: map[string]struct{}{},
	}
	return r.msgRouter(chainId, msg)
}

// msgRouter
// @Description: 消息路由转发，通过其他节点，将消息路由到目的节点
// @receiver r
// @param chainId
// @param msg
// @return ${return_type}
func (r *RouterController) msgRouter(chainId string, msg *RouteMsg) error {
	if r.maxStepCount <= 0 {
		return errors.New("maxStepCount is not set")
	}
	if len(msg.Middleman) > r.maxStepCount {
		r.log.Debugf("step(%v)>ROUTER_MAX_STEP(%v) msgId(%v)", len(msg.Middleman), r.maxStepCount, msg.MsgId)
		return nil
	}
	if _, ok := msg.Middleman[r.localPeer]; ok {
		return errors.New("msg to(" + msg.To.Pretty() + ") send already")
	}
	msg.Middleman[r.localPeer] = struct{}{}
	raw, err := json.Marshal(msg)
	if err != nil {
		r.log.Error("msgRouter json err:", err)
		return err
	}
	if r.ln.libP2pHost.connManager.IsConnected(msg.To) {
		return r.sendMsg(chainId, msg.To, TOPIC_MSG_ROUTER, raw)
	}
	peerId, err := r.ln.libP2pHost.connManager.GetRandomConnExcept(msg.Middleman)
	if err != nil {
		return errors.New("msg to(" + msg.To.Pretty() + ") no conn to use")
	}
	if _, ok := msg.Middleman[peerId.Pretty()]; ok {
		return errors.New("msg to(" + msg.To.Pretty() + ") is loop")
	}
	return r.sendMsg(chainId, peerId, TOPIC_MSG_ROUTER, raw)
}

// sendMsg
// @Description: 发送消息
// @receiver r
// @param chainId
// @param pid
// @param msgFlag
// @param data
// @return ${return_type}
func (r *RouterController) sendMsg(chainId string, pid peer.ID, msgFlag string, data []byte) error {
	// is peer belong to this chain
	ln := r.ln
	if !ln.prepare.isInsecurity && !ln.libP2pHost.peerChainIdsRecorder.IsPeerBelongToChain(pid.Pretty(), chainId) {
		return ErrorNotBelongToChain
	}
	// whether pkt adapter enable
	if ln.pktAdapter != nil {
		return ln.pktAdapter.sendMsg(chainId, pid, msgFlag, data)
	}
	return r.ln.sendMsg(chainId, pid, TOPIC_MSG_ROUTER, data)
}
