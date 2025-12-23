/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	protocol "techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-engine/v2/config"
	"techtradechain.com/techtradechain/vm-engine/v2/interfaces"
	"techtradechain.com/techtradechain/vm-engine/v2/pb/protogo"
	"techtradechain.com/techtradechain/vm-engine/v2/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ContractEngineClient .
type ContractEngineClient struct {
	id          uint64
	clientMgr   interfaces.ContractEngineClientMgr
	lock        *sync.RWMutex
	stream      protogo.DockerVMRpc_DockerVMCommunicateClient
	logger      protocol.Logger
	stopSend    chan struct{}
	stopReceive chan struct{}
	config      *config.DockerVMConfig
}

// NewContractEngineClient .
func NewContractEngineClient(
	id uint64,
	logger protocol.Logger,
	cm interfaces.ContractEngineClientMgr,
) *ContractEngineClient {

	return &ContractEngineClient{
		id:          id,
		clientMgr:   cm,
		lock:        &sync.RWMutex{},
		stream:      nil,
		logger:      logger,
		stopSend:    make(chan struct{}),
		stopReceive: make(chan struct{}),
		config:      cm.GetVMConfig(),
	}
}

// Start .
func (c *ContractEngineClient) Start() error {

	c.logger.Infof("start contract engine client[%d]", c.id)
	conn, err := c.NewClientConn()
	if err != nil {
		c.logger.Errorf("client[%d] fail to create connection: %s", c.id, err)
		return err
	}

	stream, err := GetClientStream(conn)
	if err != nil {
		c.logger.Warnf("fail to get connection stream: %s", err)
		closeErr := conn.Close()
		if closeErr != nil {
			c.logger.Warnf("connection close error: %s", closeErr)
		}
		return err
	}

	c.stream = stream

	go func() {
		select {
		case <-c.stopReceive:
			if err = conn.Close(); err != nil {
				c.logger.Warnf("failed to close connection")
			}
		case <-c.stopSend:
			if err = conn.Close(); err != nil {
				c.logger.Warnf("failed to close connection")
			}
		}
	}()

	go c.sendMsgRoutine()

	go c.receiveMsgRoutine()

	return nil
}

// Stop .
func (c *ContractEngineClient) Stop() {
	err := c.stream.CloseSend()
	if err != nil {
		c.logger.Errorf("close stream failed: ", err)
	}
}

func (c *ContractEngineClient) sendMsgRoutine() {

	c.logger.Debugf("start sending contract engine message ")

	var err error

	for {
		select {
		case txReq := <-c.clientMgr.GetTxSendCh():
			c.logger.DebugDynamic(func() string {
				return fmt.Sprintf("[%s] send tx req, chan len: [%d]", txReq.TxId, c.clientMgr.GetTxSendChLen())
			})
			utils.EnterNextStep(txReq, protogo.StepType_RUNTIME_GRPC_SEND_TX_REQUEST, func() string {
				return strings.Join([]string{"msgSize", strconv.Itoa(txReq.Size())}, ":")
			})
			err = c.sendMsg(txReq)
		case getByteCodeResp := <-c.clientMgr.GetByteCodeRespSendCh():
			c.logger.Debugf(
				"[%s] send GetByteCode resp, chan len: [%d]",
				getByteCodeResp.TxId,
				c.clientMgr.GetByteCodeRespChLen(),
			)
			err = c.sendMsg(getByteCodeResp)
		case <-c.stopSend:
			c.logger.Debugf("close contract engine send goroutine")
			return
		}

		if err != nil {
			errStatus, _ := status.FromError(err)
			c.logger.Errorf("fail to send msg: err: %s, err massage: %s, err code: %s", err,
				errStatus.Message(), errStatus.Code())
			if errStatus.Code() != codes.ResourceExhausted {
				close(c.stopReceive)
				return
			}
		}
	}
}

func (c *ContractEngineClient) receiveMsgRoutine() {

	c.logger.Debugf("start receiving contract engine message ")
	defer func() {
		c.clientMgr.PutEvent(&interfaces.Event{
			Id:        c.id,
			EventType: interfaces.EventType_ConnectionStopped,
		})
	}()

	for {
		select {
		case <-c.stopReceive:
			c.logger.Debugf("close contract engine client receive goroutine")
			return
		default:
			msg := utils.DockerVMMessageFromPool()
			err := c.stream.RecvMsg(msg)
			if err != nil {
				c.logger.Errorf("contract engine client receive err, %s", err)
				close(c.stopSend)
				return
			}

			c.logger.Debugf("[%s] receive msg from docker manager, msg type [%s]", msg.TxId, msg.Type)

			switch msg.Type {
			case protogo.DockerVMType_TX_RESPONSE:
				notify := c.clientMgr.GetReceiveNotify(msg.ChainId, msg.TxId)
				if notify == nil {
					c.logger.Warnf("[%s] fail to retrieve notify, tx notify is nil",
						msg.TxId)
					continue
				}
				notify(msg)
			case protogo.DockerVMType_GET_BYTECODE_REQUEST:
				notify := c.clientMgr.GetReceiveNotify(msg.ChainId, msg.TxId)
				if notify == nil {
					c.logger.Warnf("[%s] fail to retrieve notify, tx notify is nil", msg.TxId)
					continue
				}
				notify(msg)

			case protogo.DockerVMType_ERROR:
				notify := c.clientMgr.GetReceiveNotify(msg.ChainId, msg.TxId)
				if notify == nil {
					c.logger.Warnf("[%s] fail to retrieve notify, tx notify is nil", msg.TxId)
					continue
				}
				notify(msg)

			default:
				c.logger.Errorf("unknown message type, received msg: [%v]", msg)
			}
			//msg.ReturnToPool()
		}
	}
}

func (c *ContractEngineClient) sendMsg(msg *protogo.DockerVMMessage) error {
	c.logger.DebugDynamic(func() string {
		return fmt.Sprintf("send message[%s], type: [%s]", msg.TxId, msg.Type)
	})
	//c.logger.Debugf("msg [%+v]", msg)
	return c.stream.Send(msg)
}

// NewClientConn create rpc connection
func (c *ContractEngineClient) NewClientConn() (*grpc.ClientConn, error) {

	// just for mac development and pprof testing
	dialOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(int(utils.GetMaxRecvMsgSizeFromConfig(c.config))*1024*1024),
			grpc.MaxCallSendMsgSize(int(utils.GetMaxSendMsgSizeFromConfig(c.config))*1024*1024),
		),
		grpc.WithWriteBufferSize(4 * 1024 * 1024),
		grpc.WithReadBufferSize(4 * 1024 * 1024),
		grpc.WithInitialWindowSize(64 * 1024 * 1024),
		grpc.WithInitialConnWindowSize(64 * 1024 * 1024),
	}
	if c.config.ConnectionProtocol == config.UDSProtocol {
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, sock string) (net.Conn, error) {
			unixAddress, _ := net.ResolveUnixAddr("unix", sock)
			conn, err := net.DialUnix("unix", nil, unixAddress)
			return conn, err
		}))

		sockAddress := filepath.Join(c.config.DockerVMMountPath, config.SockDir, config.EngineSockName)

		return grpc.DialContext(context.Background(), sockAddress, dialOpts...)
	}

	url := fmt.Sprintf("%s:%d", c.config.ContractEngine.Host, c.config.ContractEngine.Port)

	return grpc.Dial(url, dialOpts...)

}

// GetClientStream get rpc stream
func GetClientStream(conn *grpc.ClientConn) (protogo.DockerVMRpc_DockerVMCommunicateClient, error) {
	return protogo.NewDockerVMRpcClient(conn).DockerVMCommunicate(context.Background())
}
