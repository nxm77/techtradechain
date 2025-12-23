/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package rpc

import (
	"context"
	"net"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"techtradechain.com/techtradechain/vm-docker-go/v2/utils"

	"techtradechain.com/techtradechain/logger/v2"
	"techtradechain.com/techtradechain/vm-docker-go/v2/config"
	"techtradechain.com/techtradechain/vm-docker-go/v2/pb/protogo"
	"google.golang.org/grpc"
)

type ClientMgr interface {
	GetTxSendCh() chan *protogo.CDMMessage

	GetSysCallRespSendCh() chan *protogo.CDMMessage

	GetAndDeleteReceiveChan(chainId, txId string) chan *protogo.CDMMessage

	GetReceiveChan(chainId, txId string) chan *protogo.CDMMessage

	GetVMConfig() *config.DockerVMConfig

	PutEvent(event *Event)
}

type CDMClient struct {
	id          uint64
	clientMgr   ClientMgr
	stream      protogo.CDMRpc_CDMCommunicateClient
	logger      *logger.CMLogger
	stopSend    chan struct{}
	stopReceive chan struct{}
}

func NewCDMClient(_id uint64, _logger *logger.CMLogger, _clientMgr ClientMgr) *CDMClient {

	return &CDMClient{
		id:          _id,
		clientMgr:   _clientMgr,
		stream:      nil,
		logger:      _logger,
		stopSend:    make(chan struct{}),
		stopReceive: make(chan struct{}),
	}
}

func (c *CDMClient) StartClient() error {

	c.logger.Infof("start cdm client[%d]", c.id)
	conn, err := c.NewClientConn()
	if err != nil {
		c.logger.Errorf("client[%d] fail to create connection: %s", c.id, err)
		return err
	}

	stream, err := GetCDMClientStream(conn)
	if err != nil {
		c.logger.Errorf("client[%d] fail to get connection stream: %s", c.id, err)
		_ = conn.Close()
		return err
	}

	c.stream = stream

	// close connection if send goroutine or receive goroutine exit
	go func() {
		select {
		case <-c.stopReceive:
			_ = conn.Close()
		case <-c.stopSend:
			_ = conn.Close()
		}
	}()

	go c.sendMsgRoutine()

	go c.receiveMsgRoutine()

	return nil
}

func (c *CDMClient) StopSendRecv() {
	err := c.stream.CloseSend()
	if err != nil {
		c.logger.Errorf("close stream failed: ", err)
	}
}

func (c *CDMClient) sendMsgRoutine() {

	c.logger.Infof("client[%d] start sending cdm message", c.id)

	var err error

	for {
		select {
		case txMsg := <-c.clientMgr.GetTxSendCh():
			c.logger.Debugf("client[%d] [%s] send tx req, chan len: [%d]", c.id, txMsg.TxId,
				len(c.clientMgr.GetTxSendCh()))
			err = c.sendCDMMsg(txMsg)
		case stateMsg := <-c.clientMgr.GetSysCallRespSendCh():
			c.logger.Debugf("client[%d] [%s] send syscall resp, chan len: [%d]", c.id, stateMsg.TxId,
				len(c.clientMgr.GetSysCallRespSendCh()))
			err = c.sendCDMMsg(stateMsg)
		case <-c.stopSend:
			c.logger.Debugf("client[%d] close cdm send goroutine", c.id)
			return
		}

		if err != nil {
			errStatus, _ := status.FromError(err)
			c.logger.Errorf("client[%d] fail to send msg: err: %s, err massage: %s, err code: %s", c.id, err,
				errStatus.Message(), errStatus.Code())
			if errStatus.Code() != codes.ResourceExhausted {
				close(c.stopReceive)
				return
			}
		}
	}
}

func (c *CDMClient) receiveMsgRoutine() {

	c.logger.Infof("client[%d] start receiving cdm message", c.id)

	defer func() {
		c.clientMgr.PutEvent(&Event{
			id:        c.id,
			eventType: connectionStopped,
		})
	}()

	var waitCh chan *protogo.CDMMessage

	for {

		select {
		case <-c.stopReceive:
			c.logger.Debugf("client[%d] close cdm client receive goroutine", c.id)
			return
		default:
			receivedMsg, revErr := c.stream.Recv()

			if revErr != nil {
				c.logger.Errorf("client[%d] receive err and exit receive goroutine %s", c.id, revErr)
				close(c.stopSend)
				return
			}

			c.logger.Debugf("client[%d] receive msg from docker manager [%s]", c.id, receivedMsg.TxId)

			switch receivedMsg.Type {
			case protogo.CDMType_CDM_TYPE_TX_RESPONSE:
				waitCh = c.clientMgr.GetAndDeleteReceiveChan(receivedMsg.ChainId, receivedMsg.TxId)
				if waitCh == nil {
					c.logger.Warnf("client[%d] [%s] fail to retrieve response chan, tx response chan is nil",
						c.id, receivedMsg.TxId)
					continue
				}
				waitCh <- receivedMsg
			case protogo.CDMType_CDM_TYPE_GET_STATE, protogo.CDMType_CDM_TYPE_GET_BATCH_STATE,
				protogo.CDMType_CDM_TYPE_GET_BYTECODE, protogo.CDMType_CDM_TYPE_CREATE_KV_ITERATOR,
				protogo.CDMType_CDM_TYPE_CONSUME_KV_ITERATOR, protogo.CDMType_CDM_TYPE_CREATE_KEY_HISTORY_ITER,
				protogo.CDMType_CDM_TYPE_CONSUME_KEY_HISTORY_ITER, protogo.CDMType_CDM_TYPE_GET_SENDER_ADDRESS,
				protogo.CDMType_CDM_TYPE_GET_CONTRACT_NAME:
				waitCh = c.clientMgr.GetReceiveChan(receivedMsg.ChainId, receivedMsg.TxId)
				if waitCh == nil {
					c.logger.Warnf("client[%d] [%s] fail to retrieve response chan, response chan is nil", c.id,
						receivedMsg.TxId)
					continue
				}
				waitCh <- receivedMsg
			default:
				c.logger.Errorf("client[%d] unknown message type, received msg: [%v]", c.id, receivedMsg)
			}
		}
	}
}

func (c *CDMClient) sendCDMMsg(msg *protogo.CDMMessage) error {
	c.logger.Debugf("client[%d] send message: [%s], type: [%s]", c.id, msg.TxId, msg.Type)
	return c.stream.Send(msg)
}

// NewClientConn create rpc connection
func (c *CDMClient) NewClientConn() (*grpc.ClientConn, error) {

	dialOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(int(utils.GetMaxRecvMsgSizeFromConfig(c.clientMgr.GetVMConfig())*1024*1024)),
			grpc.MaxCallSendMsgSize(int(utils.GetMaxSendMsgSizeFromConfig(c.clientMgr.GetVMConfig())*1024*1024)),
		),
	}

	// connect vm from unix domain socket address
	if c.clientMgr.GetVMConfig().DockerVMUDSOpen {
		// connect unix domain socket
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, sock string) (net.Conn, error) {
			unixAddress, _ := net.ResolveUnixAddr("unix", sock)
			conn, err := net.DialUnix("unix", nil, unixAddress)
			return conn, err
		}))

		sockAddress := filepath.Join(c.clientMgr.GetVMConfig().DockerVMMountPath, config.SockDir, config.SockName)

		c.logger.Infof("connect docker vm manager: %s", sockAddress)
		return grpc.DialContext(context.Background(), sockAddress, dialOpts...)
	}

	// connect vm from tcp
	url := utils.GetURLFromConfig(c.clientMgr.GetVMConfig())
	c.logger.Infof("connect docker vm manager: %s", url)
	return grpc.Dial(url, dialOpts...)
}

// GetCDMClientStream get rpc stream
func GetCDMClientStream(conn *grpc.ClientConn) (protogo.CDMRpc_CDMCommunicateClient, error) {
	return protogo.NewCDMRpcClient(conn).CDMCommunicate(context.Background())
}
