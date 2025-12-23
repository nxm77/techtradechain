/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"

	"techtradechain.com/techtradechain/common/v2/ca"
	"techtradechain.com/techtradechain/pb-go/v2/api"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

const (
	networkClientRetryInterval = 500 // 获取可用客户端连接对象重试时间间隔，单位：ms
	networkClientRetryLimit    = 5   // 获取可用客户端连接对象最大重试次数
	firstOptimizeDetectionTime = 5   // 第一次进行乐观检查时的启动时间
)

var _ ConnectionPool = (*ClientConnectionPool)(nil)

// TxRequestCreator 交易生成器
type TxRequestCreator interface {
	// CreateGetLastBlockTxRequest 获取最新区块的请求
	CreateGetLastBlockTxRequest() (*common.TxRequest, error)
}

// ConnectionPool grpc connection pool interface
type ConnectionPool interface {
	// startOptimizeDetection 启动乐观检查，该函数会定时检查连接的状态，将最靠前的连接往前放
	startOptimizeDetection(ctx context.Context, optimizeDetectionSeconds int, trCreator TxRequestCreator)
	initGRPCConnect(nodeAddr string, useTLS bool, caPaths, caCerts []string, tlsHostName string) (*grpc.ClientConn, error)
	getClient() (*networkClient, error)
	getClientWithIgnoreAddrs(ignoreAddrs map[string]struct{}) (*networkClient, error)
	getLogger() utils.Logger
	Close() error
}

// networkClients 连接客户端集合
type networkClients struct {
	nodeAddr string
	clients  []*networkClient
}

// newNetworkClients 创建网络客户端连接集合
func newNetworkClients(nodeAddr string, client *networkClient) *networkClients {
	clis := &networkClients{
		nodeAddr: nodeAddr,
		clients:  make([]*networkClient, 0),
	}
	clis.clients = append(clis.clients, client)
	return clis
}

// add 添加client
func (cs *networkClients) add(client *networkClient) {
	cs.clients = append(cs.clients, client)
}

// randomGet 随机返回一个客户端
// nolint:gosec
func (cs *networkClients) randomGet() *networkClient {
	return cs.clients[rand.Intn(len(cs.clients))]
}

// shuffle 数组打乱
func (cs *networkClients) shuffle() {
	cs.clients = shuffle(cs.clients)
}

// networkClient 客户端连接结构定义
type networkClient struct {
	rpcNode           api.RpcNodeClient
	conn              *grpc.ClientConn
	innerID           int
	nodeAddr          string
	useTLS            bool
	caPaths           []string
	caCerts           []string
	tlsHostName       string
	chainTlsHostName  string
	ID                string
	rpcMaxRecvMsgSize int
	rpcMaxSendMsgSize int
}

func (cli *networkClient) sendRequest(txReq *common.TxRequest, timeout int64) (*common.TxResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel() // releases resources if SendRequest completes before timeout elapses
	return cli.rpcNode.SendRequest(ctx, txReq)
}

func (cli *networkClient) sendRequestSync(txReq *common.TxRequest, timeout int64) (*common.TxResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel() // releases resources if SendRequest completes before timeout elapses
	return cli.rpcNode.SendRequestSync(ctx, txReq)
}

// ClientConnectionPool 客户端连接池结构定义
type ClientConnectionPool struct {
	// mut protect connections
	mut               sync.RWMutex
	connections       []*networkClient
	logger            utils.Logger
	userKeyBytes      []byte
	userCrtBytes      []byte
	userEncKeyBytes   []byte
	userEncCrtBytes   []byte
	rpcMaxRecvMsgSize int
	rpcMaxSendMsgSize int
	proxyUrl          string
	stopped           bool
	stopC             chan struct{}
}

// NewConnPool 创建连接池
func NewConnPool(config *ChainClientConfig) (*ClientConnectionPool, error) {
	pool := &ClientConnectionPool{
		logger:            config.logger,
		userKeyBytes:      config.userKeyBytes,
		userCrtBytes:      config.userCrtBytes,
		userEncKeyBytes:   config.userEncKeyBytes,
		userEncCrtBytes:   config.userEncCrtBytes,
		rpcMaxRecvMsgSize: config.rpcClientConfig.rpcClientMaxReceiveMessageSize * 1024 * 1024,
		rpcMaxSendMsgSize: config.rpcClientConfig.rpcClientMaxSendMessageSize * 1024 * 1024,
		proxyUrl:          config.proxyUrl,
		stopC:             make(chan struct{}, 1),
	}

	for idx, node := range config.nodeList {
		for i := 0; i < node.connCnt; i++ {
			cli := &networkClient{
				innerID:           i,
				nodeAddr:          node.addr,
				useTLS:            node.useTLS,
				caPaths:           node.caPaths,
				caCerts:           node.caCerts,
				tlsHostName:       node.tlsHostName,
				chainTlsHostName:  node.chainTlsHostName,
				ID:                fmt.Sprintf("%v-%v-%v", idx, node.addr, node.tlsHostName),
				rpcMaxRecvMsgSize: pool.rpcMaxRecvMsgSize,
				rpcMaxSendMsgSize: pool.rpcMaxSendMsgSize,
			}
			pool.connections = append(pool.connections, cli)
		}
	}

	// 打散，用作负载均衡
	pool.connections = shuffle(pool.connections)

	return pool, nil
}

// NewCanonicalTxFetcherPools 创建连接池
func NewCanonicalTxFetcherPools(config *ChainClientConfig) (map[string]ConnectionPool, error) {
	var pools = make(map[string]ConnectionPool)
	for idx, node := range config.nodeList {
		pool := &ClientConnectionPool{
			logger:            config.logger,
			userKeyBytes:      config.userKeyBytes,
			userCrtBytes:      config.userCrtBytes,
			rpcMaxRecvMsgSize: config.rpcClientConfig.rpcClientMaxReceiveMessageSize * 1024 * 1024,
			rpcMaxSendMsgSize: config.rpcClientConfig.rpcClientMaxSendMessageSize * 1024 * 1024,
			proxyUrl:          config.proxyUrl,
			stopC:             make(chan struct{}, 1),
		}
		for i := 0; i < node.connCnt; i++ {
			cli := &networkClient{
				innerID:           i,
				nodeAddr:          node.addr,
				useTLS:            node.useTLS,
				caPaths:           node.caPaths,
				caCerts:           node.caCerts,
				tlsHostName:       node.tlsHostName,
				chainTlsHostName:  node.chainTlsHostName,
				ID:                fmt.Sprintf("%v-%v-%v", idx, node.addr, node.tlsHostName),
				rpcMaxRecvMsgSize: pool.rpcMaxRecvMsgSize,
				rpcMaxSendMsgSize: pool.rpcMaxSendMsgSize,
			}
			pool.connections = append(pool.connections, cli)
		}
		// 打散，用作负载均衡
		pool.connections = shuffle(pool.connections)
		pools[node.addr] = pool
	}
	return pools, nil
}

// 增加header拦截器（单次）
func metadataInterceptor(serverName string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req interface{}, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if serverName != "" {
			// 将参数作为元数据的一部分传递
			md := metadata.Pairs(
				"X-Server-Name", serverName,
			)
			// 将元数据添加到上下文中
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		// 调用实际的 gRPC 方法
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// 增加header拦截器（流式）
func streamInterceptor(serverName string) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// 将参数作为元数据的一部分传递
		md := metadata.Pairs(
			"X-Server-Name", serverName,
		)
		// 将元数据添加到上下文中
		ctx = metadata.NewOutgoingContext(ctx, md)

		// 调用实际的 gRPC 方法
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// startOptimizeDetection 启动乐观处理检测机制
func (pool *ClientConnectionPool) startOptimizeDetection(ctx context.Context, optimizeDetectionSeconds int,
	trCreator TxRequestCreator) {
	go func() {
		odStart := false
		firstOdC := time.After(time.Duration(firstOptimizeDetectionTime) * time.Second)
		ticker := time.NewTicker(time.Duration(optimizeDetectionSeconds) * time.Second)
		for {
			select {
			case <-firstOdC:
				// 触发第一次检查
				if odStart {
					// 已经触发，则不再处理
					break
				}
				pool.logger.Infof("first optimize detection handler start")
				// 首先判断是否已停止
				pool.mut.RLock()
				if pool.stopped {
					// 已停止，则不处理，直接退出
					pool.mut.RUnlock()
					pool.logger.Infof("first optimize detection thread stop when pool is stopped")
					return
				}
				pool.mut.RUnlock()
				// 进行乐观处理检查
				pool.mut.Lock()
				pool.optimizeDetection(trCreator)
				pool.mut.Unlock()
				pool.logger.Infof("first optimize detection handler end")
			case <-ticker.C:
				odStart = true
				pool.logger.Infof("optimize detection handler start")
				// 首先判断是否已停止
				pool.mut.RLock()
				if pool.stopped {
					// 已停止，则不处理，直接退出
					pool.mut.RUnlock()
					pool.logger.Infof("optimize detection thread stop when pool is stopped")
					return
				}
				pool.mut.RUnlock()
				// 进行乐观处理检查
				pool.mut.Lock()
				pool.optimizeDetection(trCreator)
				pool.mut.Unlock()
				pool.logger.Infof("optimize detection handler end")
			case <-ctx.Done():
				// 外层退出，则关闭定时器
				ticker.Stop()
				pool.logger.Infof("optimize detection thread stop when ctx is done")
				return
			case <-pool.stopC:
				// 该pool已被关闭
				ticker.Stop()
				pool.logger.Infof("optimize detection thread stop when pool is closed")
				return
			}
		}
	}()
}

// optimizeDetection 乐观处理检测
func (pool *ClientConnectionPool) optimizeDetection(trCreator TxRequestCreator) {
	// 生成node->[]client的map，便于处理每个节点（每个节点的连接仅需要处理一个有用连接即可）
	availableClients := make(map[string]*networkClients)
	// 定义不用用的连接，为后续添加使用
	unavailableConns := make([]*networkClient, 0)
	for _, cli := range pool.connections {
		nodeAddr := cli.nodeAddr
		// 本连接不可用，尝试后不可用则加入到不可用集合中
		if cli.conn == nil || cli.conn.GetState() == connectivity.Shutdown {
			pool.logger.Warnf("%s connection is unavailable which conn is or shutdown", cli.innerID)
			// 为防止因为第一次无法连接而持续使得该连接饥饿，需要对其进行一次尝试
			var (
				conn *grpc.ClientConn
				err  error
			)
			conn, err = pool.initGRPCConnectWithHostName(cli.nodeAddr, cli.useTLS, cli.caPaths, cli.caCerts,
				cli.tlsHostName, cli.chainTlsHostName)
			if err != nil {
				pool.logger.Errorf("init grpc connection [nodeAddr:%s] failed, %s", cli.innerID, err.Error())
				// 仍然连不上则将其加入到不可用集合中
				unavailableConns = append(unavailableConns, cli)
				continue
			}
			// double check
			if cli.conn == nil || cli.conn.GetState() == connectivity.Shutdown {
				cli.conn = conn
				cli.rpcNode = api.NewRpcNodeClient(conn)
			}
		}
		// 判断当前连接是否可用
		if s := cli.conn.GetState(); s == connectivity.Idle || s == connectivity.Ready || s == connectivity.Connecting {
			pool.logger.Infof("%d %s connection is available, will append to available clients", cli.innerID, cli.ID)
			// 表示此节点对应连接可用，加入到map中
			if clis, ok := availableClients[nodeAddr]; ok {
				clis.add(cli)
			} else {
				availableClients[nodeAddr] = newNetworkClients(nodeAddr, cli)
			}
		} else {
			pool.logger.Warnf("%d %s connection is unavailable which state is %s", cli.innerID, cli.ID, s.String())
			// 不可用，则加入到不可用集合中
			unavailableConns = append(unavailableConns, cli)
		}
	}
	pool.logger.Infof("optimize detection start handler check connections[%d]", len(availableClients))
	txReq, err := trCreator.CreateGetLastBlockTxRequest()
	if err != nil {
		// 打印错误信息，直接返回
		pool.logger.Errorf("create get last block tx request error %s", err.Error())
		return
	}
	// 调用外部处理函数，获取排序后的结果
	sortedClients, err := pool.handleAndSortClients(availableClients, txReq)
	if err != nil {
		// 打印错误信息，直接返回
		pool.logger.Errorf("handle available clients error %s", err.Error())
		return
	}
	// 根据处理结果，更新内部的连接集合
	sortedConnections := make([]*networkClient, 0)
	for i, sortedClientArray := range sortedClients {
		// 先打乱，然后逐个添加
		sortedClientArray.shuffle()
		// 打印下优先用到的conn
		pool.logger.Infof("%d optimize detection sort answer -> %s", i, sortedClientArray.nodeAddr)
		sortedConnections = append(sortedConnections, sortedClientArray.clients...)
	}
	// 加入不可用的连接
	pool.logger.Infof("optimize detection add unavailable connections[%d]", len(unavailableConns))
	sortedConnections = append(sortedConnections, unavailableConns...)
	// 全部加入后，替换原来集合，需要加锁
	if pool.stopped {
		// 已停止，则不处理，直接退出
		return
	}
	pool.connections = sortedConnections
}

// handleAndSortClients 处理连接情况检查
func (pool *ClientConnectionPool) handleAndSortClients(availableClients map[string]*networkClients,
	txRequest *common.TxRequest) ([]*networkClients, error) {
	if txRequest == nil {
		return nil, errors.New("tx request is nil")
	}
	// 获取每个Node对应的一个连接
	sortedNetClients := make([]*networkClients, 0)
	if len(availableClients) == 0 {
		return nil, errors.New("available clients is empty")
	}
	// 用于并发测试的数据
	testClients := make([]*networkClient, 0)
	for _, clients := range availableClients {
		testClients = append(testClients, clients.randomGet())
	}
	var wg sync.WaitGroup
	handleResults := make([]*handleResult, len(testClients))
	for i := 0; i < len(testClients); i++ {
		wg.Add(1)
		currClient := testClients[i]
		go func(idx int, cli *networkClient) {
			defer wg.Done()
			nodeAddr := cli.nodeAddr
			startTime := time.Now()
			currBlockHeight := uint64(0)
			resp, err := cli.sendRequest(txRequest, OptimizeDetectionTxReqTimeout)
			elapsed := time.Since(startTime).Milliseconds()
			if err != nil {
				pool.logger.Errorf("send get last block request error: %s", err.Error())
				handleResults[idx] = newHandleResult(cli.nodeAddr, currBlockHeight, elapsed)
				return
			}
			// 检查结果
			if err = utils.CheckProposalRequestResp(resp, true); err != nil {
				pool.logger.Errorf("check get last block resp error: %s", err.Error())
				handleResults[idx] = newHandleResult(cli.nodeAddr, currBlockHeight, elapsed)
				return
			}
			blockInfo := &common.BlockInfo{}
			if err = proto.Unmarshal(resp.ContractResult.Result, blockInfo); err != nil {
				pool.logger.Errorf("get last block resp unmarshal to block info error: %s", err.Error())
				handleResults[idx] = newHandleResult(cli.nodeAddr, currBlockHeight, elapsed)
				return
			}
			currBlockHeight = blockInfo.Block.Header.BlockHeight
			handleResults[idx] = newHandleResult(nodeAddr, currBlockHeight, elapsed)
			pool.logger.Infof("handle node[%s] connection success, [%d/%d]", nodeAddr, currBlockHeight, elapsed)
		}(i, currClient)
	}
	wg.Wait()
	// 对结果进行排序
	sort.Sort(handleResultArray(handleResults))
	// 遍历排序后的结果，并将其加入到返回结果中
	for _, hr := range handleResults {
		if clis, ok := availableClients[hr.nodeAddr]; ok {
			sortedNetClients = append(sortedNetClients, clis)
		} else {
			// 打印错误
			// 理论上不会出现false的情况
			pool.logger.Errorf("can not load node addr %s", hr.nodeAddr)
		}
	}
	return sortedNetClients, nil
}

// initGRPCConnect 初始化GPRC客户端连接
func (pool *ClientConnectionPool) initGRPCConnect(nodeAddr string, useTLS bool, caPaths, caCerts []string,
	tlsHostName string) (*grpc.ClientConn, error) {
	pool.mut.Lock()
	defer pool.mut.Unlock()
	return pool.initGRPCConnectWithHostName(nodeAddr, useTLS, caPaths, caCerts, tlsHostName, "")
}

// initGRPCConnectWithHostName 初始化GPRC客户端连接
func (pool *ClientConnectionPool) initGRPCConnectWithHostName(nodeAddr string, useTLS bool, caPaths, caCerts []string,
	tlsHostName string, chainTlsHostName string) (*grpc.ClientConn, error) {
	var kacp = keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}
	var tlsClient ca.CAClient
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(pool.rpcMaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(pool.rpcMaxSendMsgSize),
		),
		grpc.WithKeepaliveParams(kacp),
	}

	if useTLS {
		if len(caCerts) != 0 {
			tlsClient = ca.CAClient{
				ServerName:   tlsHostName,
				CaCerts:      caCerts,
				CertBytes:    pool.userCrtBytes,
				KeyBytes:     pool.userKeyBytes,
				EncCertBytes: pool.userEncCrtBytes,
				EncKeyBytes:  pool.userEncKeyBytes,
				Logger:       pool.logger,
			}
		} else {
			tlsClient = ca.CAClient{
				ServerName:   tlsHostName,
				CaPaths:      caPaths,
				CertBytes:    pool.userCrtBytes,
				KeyBytes:     pool.userKeyBytes,
				EncCertBytes: pool.userEncCrtBytes,
				EncKeyBytes:  pool.userEncKeyBytes,
				Logger:       pool.logger,
			}
		}

		c, err := tlsClient.GetCredentialsByCA()
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(*c))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	// 如果proxyUrl不为空，则配置使用代理
	if pool.proxyUrl != "" {
		proxyUrl, err := url.Parse(pool.proxyUrl)
		if err != nil {
			return nil, err
		}
		d := newGRPCProxyDialer(proxyUrl)
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return d.dial(ctx, addr)
		}))
	}

	if chainTlsHostName != "" {
		interceptor := metadataInterceptor(chainTlsHostName)           // add header with unary
		streamHeaderInterceptor := streamInterceptor(chainTlsHostName) // add header with stream
		opts = append(opts, grpc.WithUnaryInterceptor(interceptor))
		opts = append(opts, grpc.WithStreamInterceptor(streamHeaderInterceptor))

	}

	return grpc.Dial(
		nodeAddr,
		opts...,
	)
}

// 获取空闲的可用客户端连接对象
func (pool *ClientConnectionPool) getClient() (*networkClient, error) {
	return pool.getClientWithIgnoreAddrs(nil)
}

func (pool *ClientConnectionPool) getClientWithIgnoreAddrs(ignoreAddrs map[string]struct{}) (*networkClient, error) {
	var nc *networkClient

	err := retry.Retry(func(uint) error {
		var err error
		nc, err = pool.getClientOnce(ignoreAddrs)
		return err
	}, strategy.Wait(networkClientRetryInterval*time.Millisecond), strategy.Limit(networkClientRetryLimit))

	if err != nil {
		return nil, err
	}
	return nc, nil
}

func (pool *ClientConnectionPool) getClientOnce(ignoreAddrs map[string]struct{}) (*networkClient, error) {
	pool.mut.Lock()
	defer pool.mut.Unlock()
	var err error
	for _, cli := range pool.connections {
		if ignoreAddrs != nil {
			if _, ok := ignoreAddrs[cli.ID]; ok {
				continue
			}
		}

		if cli.conn == nil || cli.conn.GetState() == connectivity.Shutdown {
			var conn *grpc.ClientConn
			conn, err = pool.initGRPCConnectWithHostName(cli.nodeAddr, cli.useTLS, cli.caPaths, cli.caCerts,
				cli.tlsHostName, cli.chainTlsHostName)
			if err != nil {
				pool.logger.Errorf("init grpc connection [nodeAddr:%s] failed, %s", cli.ID, err.Error())
				continue
			}

			cli.conn = conn
			cli.rpcNode = api.NewRpcNodeClient(conn)
			return cli, nil
		}

		s := cli.conn.GetState()
		if s == connectivity.Idle || s == connectivity.Ready || s == connectivity.Connecting {
			return cli, nil
		}
	}
	return nil, errors.New("grpc connections unavailable, see sdk log file for more details")
}

func (pool *ClientConnectionPool) getLogger() utils.Logger {
	return pool.logger
}

// Close 关闭连接池
func (pool *ClientConnectionPool) Close() error {
	pool.mut.Lock()
	defer pool.mut.Unlock()
	for _, c := range pool.connections {
		if c.conn == nil {
			continue
		}

		if err := c.conn.Close(); err != nil {
			pool.logger.Errorf("stop %s connection failed, %s",
				c.nodeAddr, err.Error())

			continue
		}
	}
	pool.stopped = true
	pool.stopC <- struct{}{}
	return nil
}

// nolint
// 数组打散
func shuffle(vals []*networkClient) []*networkClient {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	ret := make([]*networkClient, len(vals))
	perm := r.Perm(len(vals))
	for i, randIndex := range perm {
		ret[i] = vals[randIndex]
	}

	return ret
}
