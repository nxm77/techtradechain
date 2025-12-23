/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"techtradechain.com/techtradechain/sdk-go/v2/utils"

	"techtradechain.com/techtradechain/pb-go/v2/api"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultSwitchTimeDiff       = 10                // 默认切换的时间差，默认为10s，当超过10s没有收到新区块，且有更高的节点时才切换
	optimizeDetectionExtendTime = 10                // 在默认配置基础上延长的时间，单位s
	maxContinuousErrCount       = 10                // 订阅是连续收到error的最多数量，超过该值会退出，由客户端处理
	msgOK                       = "OK"              // 对正确已完成的字符串
	msgInvalidArgument          = "InvalidArgument" // 错误的入参
)

// SubscribeSwitchHandler 订阅切换处理器切换连接处理器
type SubscribeSwitchHandler interface {
	TxRequestCreator
	// NewSubscribeTxReq 当出现连接切换时需创建新的Payload，便于重新发请求订阅
	// 如果已经满足结果则返回nil，不会再继续发送
	NewSubscribeTxReq(lastTxReq *common.TxRequest, pushedBlockHeight uint64) (*common.TxRequest, error)
}

// SubscribeBlockSwitchHandler 对于订阅block切换的处理器
type SubscribeBlockSwitchHandler struct {
	chainCli   *ChainClient
	startBlock int64
	endBlock   int64
	withRWSet  bool
	onlyHeader bool
}

// NewSubscribeBlockSwitchHandler 创建订阅block切换的处理器
func NewSubscribeBlockSwitchHandler(chainCli *ChainClient, startBlock, endBlock int64, withRWSet,
	onlyHeader bool) *SubscribeBlockSwitchHandler {
	return &SubscribeBlockSwitchHandler{
		chainCli:   chainCli,
		startBlock: startBlock,
		endBlock:   endBlock,
		withRWSet:  withRWSet,
		onlyHeader: onlyHeader,
	}
}

// CreateGetLastBlockTxRequest 创建一个获取最新区块的交易请求
func (s *SubscribeBlockSwitchHandler) CreateGetLastBlockTxRequest() (*common.TxRequest, error) {
	return s.chainCli.CreateGetLastBlockTxRequest()
}

// NewSubscribeTxReq 创建区块订阅的交易请求
func (s *SubscribeBlockSwitchHandler) NewSubscribeTxReq(lastTxReq *common.TxRequest, pushedBlockHeight uint64) (
	*common.TxRequest, error) {
	// 判断是否已经推送了区块，0表示未推送，则使用默认配置
	if pushedBlockHeight == 0 {
		payload := s.chainCli.CreateSubscribeBlockPayload(s.startBlock, s.endBlock, s.withRWSet, s.onlyHeader)
		txReq, err := s.chainCli.GenerateTxRequest(payload, nil)
		if err != nil {
			return nil, err
		}
		return txReq, nil
	}
	// 如果目前返回的数据已经达到要求，则无须再处理，返回nil
	if int64(pushedBlockHeight) >= s.endBlock && s.endBlock != -1 {
		return nil, nil
	}
	// 此处不使用pushedBlockHeight+1，而是单纯使用pushedBlockHeight，主要是为解决低版本会报错的问题
	// 在低于v2.3.4版本时，订阅不存在的高度会报错，大于等于v2.3.4后，仅打印warn信息
	// 乐观处理已经对已收到的区块进行了处理，所以不会出现重复的情况
	payload := s.chainCli.CreateSubscribeBlockPayload(int64(pushedBlockHeight), s.endBlock, s.withRWSet, s.onlyHeader)
	txReq, err := s.chainCli.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}
	return txReq, nil
}

// SubscribeContractEventSwitchHandler 对于订阅合约事件切换的处理器
type SubscribeContractEventSwitchHandler struct {
	chainCli     *ChainClient
	startBlock   int64
	endBlock     int64
	contractName string
	topic        string
}

// NewSubscribeContractEventSwitchHandler 创建订阅合约事件切换处理器
func NewSubscribeContractEventSwitchHandler(chainCli *ChainClient, startBlock, endBlock int64, contractName,
	topic string) *SubscribeContractEventSwitchHandler {
	return &SubscribeContractEventSwitchHandler{
		chainCli:     chainCli,
		startBlock:   startBlock,
		endBlock:     endBlock,
		contractName: contractName,
		topic:        topic,
	}
}

// CreateGetLastBlockTxRequest 创建一个获取最新区块的交易请求
func (s *SubscribeContractEventSwitchHandler) CreateGetLastBlockTxRequest() (*common.TxRequest, error) {
	return s.chainCli.CreateGetLastBlockTxRequest()
}

// NewSubscribeTxReq 创建新的订阅的交易请求
func (s *SubscribeContractEventSwitchHandler) NewSubscribeTxReq(lastTxReq *common.TxRequest,
	pushedBlockHeight uint64) (*common.TxRequest, error) {
	// 判断是否已经推送了区块，0表示未推送，则使用默认配置
	if pushedBlockHeight == 0 {
		payload := s.chainCli.CreateSubscribeContractEventPayload(s.startBlock, s.endBlock, s.contractName, s.topic)
		txReq, err := s.chainCli.GenerateTxRequest(payload, nil)
		if err != nil {
			return nil, err
		}
		return txReq, nil
	}
	// 如果目前返回的数据已经达到要求，则无须再处理，返回nil
	if int64(pushedBlockHeight) >= s.endBlock && s.endBlock != -1 {
		return nil, nil
	}
	// 其他情况需要创建从已经收到区块开始的订阅请求
	// 此处不使用pushedBlockHeight+1，而是单纯使用pushedBlockHeight，主要是为解决低版本会报错的问题
	// 在低于v2.3.4版本时，订阅不存在的高度会报错，大于等于v2.3.4后，仅打印warn信息
	// 乐观处理已经对已收到的区块进行了处理，所以不会出现重复的情况
	payload := s.chainCli.CreateSubscribeContractEventPayload(int64(pushedBlockHeight),
		s.endBlock, s.contractName, s.topic)
	txReq, err := s.chainCli.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}
	return txReq, nil
}

// SubscribeBlockWithOptimize optimize block subscription, returns channel of subscribed blocks
// optimizeDetection is a connection check cycle, in seconds.
// switchTimeDiff indicates the min allowed time difference which is the time diff between two receive blocks.
// If this value is exceeded during the check, the network will be switched to the optimal one.
// If you want this call to work, the ChainClient.config.optimizeDetection must be greater than 0 and
// ChainClient.config.nodeList must be greater than 1
func (cc *ChainClient) SubscribeBlockWithOptimize(ctx context.Context, startBlock, endBlock int64, withRWSet,
	onlyHeader bool, optimizeDetection int, switchTimeDiff int64) (<-chan interface{}, error) {
	// 创建默认的payload
	payload := cc.CreateSubscribeBlockPayload(startBlock, endBlock, withRWSet, onlyHeader)
	txReq, err := cc.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}
	switchHandler := NewSubscribeBlockSwitchHandler(cc, startBlock, endBlock, withRWSet, onlyHeader)
	return cc.SubscribeWithOptimize(ctx, txReq, switchHandler, optimizeDetection, switchTimeDiff)
}

// SubscribeContractEventWithOptimize optimize contract event subscription, returns channel of
// subscribed contract events The use of optimizeDetection and switchTimeDiff can
// refer to SubscribeBlockWithOptimize
// tips: The bottom chain version must be greater than or equal to v2.3.6
func (cc *ChainClient) SubscribeContractEventWithOptimize(ctx context.Context, startBlock, endBlock int64,
	contractName, topic string, optimizeDetection int, switchTimeDiff int64) (<-chan interface{}, error) {
	// 创建默认的payload
	payload := cc.CreateSubscribeContractEventPayload(startBlock, endBlock, contractName, topic)
	txReq, err := cc.GenerateTxRequest(payload, nil)
	if err != nil {
		return nil, err
	}
	switchHandler := NewSubscribeContractEventSwitchHandler(cc, startBlock, endBlock, contractName, topic)
	return cc.SubscribeWithOptimize(ctx, txReq, switchHandler, optimizeDetection, switchTimeDiff)
}

// SubscribeWithOptimize returns channel of subscribed items with optimize way
func (cc *ChainClient) SubscribeWithOptimize(ctx context.Context, txReq *common.TxRequest,
	subscribeSwitchHandler SubscribeSwitchHandler, optimizeDetection int, switchTimeDiff int64) (
	<-chan interface{}, error) {
	method := txReq.Payload.Method
	// 如果未配置optimizeDetection或者说只有一个节点，那么使用原来的方式
	if cc.config.optimizeDetection <= 0 || len(cc.config.nodeList) <= 1 {
		// 对于订阅的事件的处理，则返回错误，因为chan中返回的结构不同
		if method == syscontract.SubscribeFunction_SUBSCRIBE_CONTRACT_EVENT.String() {
			return nil, errors.New("subscribe contract event is disabled, because config is not right")
		}
		// 其他情况走原先的逻辑
		return cc.SubscribeWithTxReq(ctx, txReq.Payload, txReq)
	}
	// 仅支持订阅区块和事件，不支持其他，其他订阅会自动将其转到普通订阅中
	if method != syscontract.SubscribeFunction_SUBSCRIBE_BLOCK.String() &&
		method != syscontract.SubscribeFunction_SUBSCRIBE_CONTRACT_EVENT.String() {
		return cc.SubscribeWithTxReq(ctx, txReq.Payload, txReq)
	}
	actualC := make(chan interface{}, subscribeBufferCap)
	stopC := make(chan struct{})
	err := cc.actualSubscribe(stopC, txReq, actualC)
	if err != nil {
		cc.logger.Errorf("[SDK] Subscriber init and start subscribe thread error:%s", err.Error())
		return nil, err
	}
	// 如果传入参数不生效，则默认使用firstOptimizeDetectionTime+10s
	if optimizeDetection <= 0 {
		optimizeDetection = firstOptimizeDetectionTime + optimizeDetectionExtendTime
	}
	if switchTimeDiff <= 0 {
		switchTimeDiff = defaultSwitchTimeDiff
	}
	// 创建定时检测定时器
	ticker := time.NewTicker(time.Duration(optimizeDetection) * time.Second)
	// 最终返回给用户的chan
	retDataC := make(chan interface{}, subscribeBufferCap)
	go func() {
		// 连续收到的数量
		continuousErrCount := 0
		// 上次接收到订阅数据的毫秒数
		lastReceivedBlockTime := int64(0)
		pushedBlockHeight := uint64(0)
		defer func() {
			// 关闭对应的定时器
			ticker.Stop()
			// 关闭通道，防止泄露
			close(actualC)
			// 关闭channel
			close(retDataC)
		}()
		// 定义上一次收到订阅信息的时刻
		for {
			select {
			case <-ticker.C:
				cc.logger.Infof("[SDK] Subscriber start optimize detection connections check")
				// 首先判断时间差是否存在，即已经有足够的时间没有收到数据了
				if (utils.CurrUnixMilli() - lastReceivedBlockTime) < switchTimeDiff*1000 {
					// 如果时间还比较小，则无需关注，也不需要切换
					cc.logger.Infof("[SDK] Subscriber last received block is still allowed, " +
						"will break current optimize detection")
					break
				}
				// 否则需要判断当前最新连接的高度情况
				// 获取最新区块高度（此处使用的为优化后的连接）
				txRequest, err := subscribeSwitchHandler.CreateGetLastBlockTxRequest()
				if err != nil {
					// 打印错误信息，直接返回
					cc.logger.Errorf("[SDK] Subscriber optimize detection create get last block tx request "+
						"error %s", err.Error())
					return
				}
				currBlockHeight, err := cc.getLastBlockHeight(txRequest)
				if err != nil {
					// 打印日志，并返回，但不退出
					// 可能仅仅是当前网络有问题，过段时间会恢复
					cc.logger.Errorf("[SDK] Subscriber optimize detection get current block err:%s", err)
					break
				}
				cc.logger.Infof("[SDK] Subscriber get last block height conditions, [%d/%d]",
					currBlockHeight, pushedBlockHeight)
				// 判断现有连接接收是否已经处于慢的情况，如果否，则不用处理
				currHeightDiff := int64(currBlockHeight - pushedBlockHeight)
				if currHeightDiff > 0 {
					// 最优连接的高度比当前连接高度高太多，需要进行连接切换
					cc.logger.Infof("[SDK] Subscriber switching connection conditions are met, [%d/%d]",
						currBlockHeight, pushedBlockHeight)
					// 生成新的请求，切换到新的节点继续尝试
					newSubscribeTxReq, err := subscribeSwitchHandler.NewSubscribeTxReq(txReq, pushedBlockHeight)
					if err != nil {
						// 无法重新订阅，直接返回
						cc.logger.Errorf("[SDK] Subscriber create new txReq err:%s", err)
						return
					}
					if newSubscribeTxReq == nil {
						// 表示满足条件，即已经返回了足够的数据给客户端，那么退出即可
						cc.logger.Info("[SDK] Subscriber received enough data and will exit")
						return
					}
					// 停止前面的通道
					close(stopC)
					// 重新开始订阅
					stopC = make(chan struct{})
					err = cc.actualSubscribe(stopC, newSubscribeTxReq, actualC)
					if err != nil {
						cc.logger.Errorf("[SDK] Subscriber restart subscription failed, err:%s", err.Error())
						// 如果此时仍存在问题，则说明最新的连接都不可用，直接退出即可
						return
					}
					cc.logger.Infof("[SDK] Subscriber restart subscription success")
					break
				}
				// 否则表示当前连接状态还可以，只需要打印日志即可
				cc.logger.Infof("[SDK] Subscriber not need switch connection, [%d/%d]", currBlockHeight, pushedBlockHeight)
			case <-ctx.Done():
				return
			case retData := <-actualC:
				// 判断类型是否是字符串
				if s, ok := retData.(string); ok {
					// 如果是字符串，存在两种情况，一种是已经收集到足够数量的交易，那么退出即可
					if s == msgOK {
						cc.logger.Info("[SDK] Subscriber received enough data and will exit")
						return
					}
					// 另外则是被服务端校验发现参数异常
					if s == msgInvalidArgument {
						cc.logger.Errorf("[SDK] Subscriber use invalid argument and will exit")
						return
					}
				}
				// 然后判断是否为error
				if e, ok := retData.(error); ok {
					// 判断连续收到err的数量是否已经超过最大允许范围
					if continuousErrCount > maxContinuousErrCount {
						cc.logger.Errorf("[SDK] Subscriber receive continuous error %d more than %d, will exit",
							continuousErrCount, maxContinuousErrCount)
						return
					}
					continuousErrCount++
					cc.logger.Infof("[SDK] Subscriber receive error from chan, %s, and will use newest connections", e.Error())
					// 生成新的请求，切换到新的节点继续尝试
					newSubscribeTxReq, err := subscribeSwitchHandler.NewSubscribeTxReq(txReq, pushedBlockHeight)
					if err != nil {
						// 无法重新订阅，直接返回
						cc.logger.Errorf("[SDK] Subscriber create new txReq err:%s", err)
						return
					}
					if newSubscribeTxReq == nil {
						// 表示满足条件，即已经返回了足够的数据给客户端，那么退出即可
						cc.logger.Info("[SDK] Subscriber received enough data and will exit")
						return
					}
					// 如果是error，表示处理的协程已经退出，不用再重新关闭
					// 延时1s，防止频繁出错
					time.Sleep(time.Second)
					// 重新开始订阅
					stopC = make(chan struct{})
					err = cc.actualSubscribe(stopC, newSubscribeTxReq, actualC)
					if err != nil {
						cc.logger.Errorf("[SDK] Subscriber restart subscription failed, err:%s", err.Error())
						// 如果此时仍存在问题，则说明最新的连接都不可用，直接退出即可
						return
					}
					cc.logger.Infof("[SDK] Subscriber restart subscription success")
					break
				}
				// 重置连续收到err的值
				continuousErrCount = 0
				// 非error情况下，都是正常数据，可根据数据类型，处理对应的高度
				switch txReq.Payload.Method {
				case syscontract.SubscribeFunction_SUBSCRIBE_BLOCK.String():
					// 判断为BlockInfo或BlockHeader
					if b, ok := retData.(*common.BlockInfo); ok {
						if pushedBlockHeight != 0 {
							// 需要保证推送的区块一定是按照顺序来的
							if b.Block.Header.BlockHeight <= pushedBlockHeight {
								cc.logger.Warnf("[SDK] Subscriber receive history block [%d/%d], will skip",
									b.Block.Header.BlockHeight, pushedBlockHeight)
								break
							}
							// 新的区块必须要比已推送的区块大1个，否则表示出现异常
							if b.Block.Header.BlockHeight != pushedBlockHeight+1 {
								cc.logger.Errorf("[SDK] Subscriber receive wrong block [%d/%d], will exit",
									b.Block.Header.BlockHeight, pushedBlockHeight)
								return
							}
						}
						// 如果是block，则获取其高度
						pushedBlockHeight = b.Block.Header.BlockHeight
						lastReceivedBlockTime = utils.CurrUnixMilli()
						retDataC <- b
						break
					}
					if b, ok := retData.(*common.BlockHeader); ok {
						if pushedBlockHeight != 0 {
							// 需要保证推送的区块一定是按照顺序来的
							if b.BlockHeight <= pushedBlockHeight {
								cc.logger.Warnf("[SDK] Subscriber receive history block header [%d/%d], will skip",
									b.BlockHeight, pushedBlockHeight)
								break
							}
							// 新的区块必须要比已推送的区块大1个，否则表示出现异常
							if b.BlockHeight != pushedBlockHeight+1 {
								cc.logger.Errorf("[SDK] Subscriber receive wrong block header [%d/%d], will exit",
									b.BlockHeight, pushedBlockHeight)
								return
							}
						}
						// 如果是block，则获取其高度
						pushedBlockHeight = b.BlockHeight
						lastReceivedBlockTime = utils.CurrUnixMilli()
						retDataC <- b
						break
					}
					cc.logger.Errorf("[SDK] Subscriber receive data type is wrong, "+
						"need BlockInfo or BlockHeader, but %T", retData)
					// 返回的类型不对，直接退出
					return
				case syscontract.SubscribeFunction_SUBSCRIBE_CONTRACT_EVENT.String():
					// 判断为ContractEventInfoList
					if ces, ok := retData.(*common.ContractEventInfoList); ok {
						// 如果是ContractEventInfoList，则获取第一个事件的高度
						if len(ces.ContractEvents) <= 0 {
							cc.logger.Warnf("[SDK] Subscriber receive contract events, but not contains block, " +
								"maybe chain is old version")
							break
						}
						currBlockHeight := ces.ContractEvents[0].BlockHeight
						if pushedBlockHeight != 0 {
							// 需要保证推送的区块一定是按照顺序来的
							if currBlockHeight <= pushedBlockHeight {
								cc.logger.Warnf("[SDK] Subscriber receive history event block [%d/%d], will skip",
									currBlockHeight, pushedBlockHeight)
								break
							}
							// 新的区块必须要比已推送的区块大1个，否则表示出现异常
							if currBlockHeight != pushedBlockHeight+1 {
								cc.logger.Errorf("[SDK] Subscriber receive wrong event block [%d/%d], will exit",
									currBlockHeight, pushedBlockHeight)
								return
							}
						}
						pushedBlockHeight = currBlockHeight
						lastReceivedBlockTime = utils.CurrUnixMilli()
						retDataC <- ces
						break
					}
					cc.logger.Errorf("[SDK] Subscriber receive data type is wrong, "+
						"need ContractEventInfoList, but %T", retData)
					// 返回的类型不对，直接退出
					return
				default:
					// 不应该返回其他信息，此处与原逻辑保持一致，返回结果，但不更新高度，并打印日志
					cc.logger.Warnf("[SDK] Subscriber unexpect method, client need check!!!")
				}
			}
		}
	}()
	return retDataC, nil
}

func (cc *ChainClient) getLastBlockHeight(txReq *common.TxRequest) (uint64, error) {
	client, err := cc.pool.getClient()
	if err != nil {
		cc.logger.Errorf("[SDK] Subscriber optimize detection get client error %s", err.Error())
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.rpcNode.SendRequestSync(ctx, txReq)
	if err != nil {
		cc.logger.Errorf("[SDK] Subscriber optimize detection send request error %s", err.Error())
		return 0, err
	}
	if err = utils.CheckProposalRequestResp(resp, true); err != nil {
		cc.logger.Errorf("[SDK] Subscriber optimize detection check resp error %s", err.Error())
		return 0, err
	}
	blockInfo := &common.BlockInfo{}
	if err = proto.Unmarshal(resp.ContractResult.Result, blockInfo); err != nil {
		cc.logger.Errorf("[SDK] Subscriber optimize detection unmarshal block info error %s", err.Error())
		return 0, err
	}
	return blockInfo.Block.Header.BlockHeight, nil
}

// actualSubscribe returns channel of subscribed items with optimize way
func (cc *ChainClient) actualSubscribe(stopC chan struct{}, txReq *common.TxRequest,
	actualRetC chan interface{}) error {
	// 创建上下文，便于后面关闭
	actualCtx, actualCancelFunc := context.WithCancel(context.Background())
	client, err := cc.pool.getClient()
	if err != nil {
		actualCancelFunc()
		return err
	}
	resp, err := client.rpcNode.Subscribe(actualCtx, txReq)
	if err != nil {
		actualCancelFunc()
		return err
	}
	// 单独启动一个goroutine，处理resp接收的情况
	go func() {
		for {
			result, err := resp.Recv()
			if err != nil && err == io.EOF {
				cc.logger.Errorf("[SDK] Subscriber got EOF and stop recv msg")
				// 将err加入chan
				actualRetC <- err
				return
			}
			if err != nil {
				cc.logger.Errorf("[SDK] Subscriber receive failed, %s", err)
				if statusErr, ok := status.FromError(err); ok {
					// 如果是被客户端显式取消，则不需要返回错误，是正常切换导致，不需要特殊处理
					if statusErr.Code() == codes.Canceled {
						cc.logger.Warnf("[SDK] Subscriber cancel typically by the client %s", err.Error())
						return
					}
					if statusErr.Code() == codes.Unknown &&
						strings.Contains(err.Error(), "malformed header: missing HTTP content-type") {
						cc.logger.Info("[SDK] Subscriber conn corrupted, close and reinit conn")
						err1 := client.conn.Close()
						if err1 != nil {
							cc.logger.Errorf("[SDK] Subscriber close grpc connection [%s] failed, %s",
								client.ID, err1.Error())
							// 将err加入chan
							actualRetC <- err1
							return
						}
						conn, err1 := cc.pool.initGRPCConnect(client.nodeAddr, client.useTLS, client.caPaths,
							client.caCerts, client.tlsHostName)
						if err1 != nil {
							cc.logger.Errorf("[SDK] Subscriber init grpc connection [%s] failed, %s",
								client.ID, err1.Error())
							// 将err加入chan
							actualRetC <- err1
							return
						}
						client.conn = conn
						client.rpcNode = api.NewRpcNodeClient(conn)
						// 将err加入chan
						actualRetC <- err
						return
					}
					if statusErr.Code() == codes.OK {
						cc.logger.Infof("[SDK] Subscriber receive enough data %s", err.Error())
						// 将字符串放进去
						actualRetC <- msgOK
						return
					}
					if statusErr.Code() == codes.InvalidArgument {
						cc.logger.Errorf("[SDK] Subscriber use invalid argument %s", err.Error())
						// 将字符串放进去
						actualRetC <- msgInvalidArgument
						return
					}
				}
				// 将err加入chan
				actualRetC <- err
				return
			}

			var ret interface{}
			switch txReq.Payload.Method {
			case syscontract.SubscribeFunction_SUBSCRIBE_BLOCK.String():
				blockInfo := &common.BlockInfo{}
				if err = proto.Unmarshal(result.Data, blockInfo); err == nil {
					ret = blockInfo
					break
				}
				blockHeader := &common.BlockHeader{}
				if err = proto.Unmarshal(result.Data, blockHeader); err == nil {
					ret = blockHeader
					break
				}
				if err != nil {
					cc.logger.Error("[SDK] Subscriber receive block failed, %s", err)
					// 将err加入chan
					actualRetC <- err
					return
				}
			case syscontract.SubscribeFunction_SUBSCRIBE_TX.String():
				tx := &common.Transaction{}
				if err = proto.Unmarshal(result.Data, tx); err != nil {
					cc.logger.Error("[SDK] Subscriber receive tx failed, %s", err)
					// 将err加入chan
					actualRetC <- err
					return
				}
				ret = tx
			case syscontract.SubscribeFunction_SUBSCRIBE_CONTRACT_EVENT.String():
				events := &common.ContractEventInfoList{}
				if err = proto.Unmarshal(result.Data, events); err != nil {
					cc.logger.Error("[SDK] Subscriber receive contract event failed, %s", err)
					// 将err加入chan
					actualRetC <- err
					return
				}
				ret = events
			default:
				ret = result.Data
			}
			actualRetC <- ret
		}
	}()
	go func() {
		defer actualCancelFunc()
		_, ok := <-stopC
		if !ok {
			cc.logger.Warn("[SDK] Subscriber exit because the external close stopC")
		} else {
			cc.logger.Warn("[SDK] Subscriber exit because the external call stopC")
		}
	}()
	return nil
}
