package rpc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	protocol "techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-engine/v2/config"
	"techtradechain.com/techtradechain/vm-engine/v2/pb/protogo"
	"techtradechain.com/techtradechain/vm-engine/v2/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	runtimeServerOnce     sync.Once
	runtimeServerInstance *RuntimeServer
	//runtimeServerStartOnce sync.Once
)

// RuntimeServer server for sandbox
type RuntimeServer struct {
	listener  net.Listener
	rpcServer *grpc.Server
	config    *config.DockerVMConfig
	logger    protocol.Logger
	startOnce sync.Once
	stopOnce  sync.Once
}

// NewRuntimeServer .
func NewRuntimeServer(logger protocol.Logger, vmConfig *config.DockerVMConfig) (*RuntimeServer, error) {
	errCh := make(chan error, 1)

	runtimeServerOnce.Do(func() {
		if vmConfig == nil {
			errCh <- errors.New("invalid parameter, config is nil")
			return
		}

		listener, err := createListener(vmConfig)
		if err != nil {
			errCh <- err
			return
		}

		// set up server options for keepalive and TLS
		var serverOpts []grpc.ServerOption

		// add keepalive
		serverKeepAliveParameters := keepalive.ServerParameters{
			Time:    1 * time.Minute,
			Timeout: 20 * time.Second,
		}
		serverOpts = append(serverOpts, grpc.KeepaliveParams(serverKeepAliveParameters))

		// set enforcement policy
		kep := keepalive.EnforcementPolicy{
			MinTime: config.ServerMinInterval,
			// allow keepalive w/o rpc
			PermitWithoutStream: true,
		}

		serverOpts = append(serverOpts, grpc.KeepaliveEnforcementPolicy(kep))

		serverOpts = append(serverOpts, grpc.ConnectionTimeout(config.ConnectionTimeout))
		serverOpts = append(serverOpts, grpc.MaxSendMsgSize(int(utils.GetMaxRecvMsgSizeFromConfig(vmConfig))*1024*1024))
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(int(utils.GetMaxRecvMsgSizeFromConfig(vmConfig))*1024*1024))
		serverOpts = append(serverOpts, grpc.ReadBufferSize(config.BufferSize))
		serverOpts = append(serverOpts, grpc.WriteBufferSize(config.BufferSize))
		serverOpts = append(serverOpts, grpc.InitialWindowSize(64*1024*1024))
		serverOpts = append(serverOpts, grpc.InitialConnWindowSize(64*1024*1024))
		serverOpts = append(serverOpts, grpc.NumStreamWorkers(uint32(runtime.NumCPU()/2))) //nolint:gosec

		server := grpc.NewServer(serverOpts...)

		runtimeServerInstance = &RuntimeServer{
			listener:  listener,
			rpcServer: server,
			logger:    logger,
			config:    vmConfig,
		}
	})

	select {
	case err := <-errCh:
		return nil, err
	default:
		return runtimeServerInstance, nil
	}
}

// StartRuntimeServer .
func (s *RuntimeServer) StartRuntimeServer(runtimeService *RuntimeService) error {
	var startErr error
	s.startOnce.Do(
		func() {
			if s.listener == nil {
				startErr = errors.New("nil listener")
				return
			}

			if s.rpcServer == nil {
				startErr = errors.New("nil server")
				return
			}

			protogo.RegisterDockerVMRpcServer(s.rpcServer, runtimeService)

			s.logger.Debug("start runtime server")
			go func() {
				err := s.rpcServer.Serve(s.listener)
				if err != nil {
					s.logger.Errorf("runtime server fail to start: %s", err)
				}
			}()
		})

	return startErr
}

// StopRuntimeServer stops runtime server
func (s *RuntimeServer) StopRuntimeServer() {
	s.stopOnce.Do(func() {
		s.logger.Info("stop runtime server")
		if s.rpcServer != nil {
			s.rpcServer.Stop()
		}
	})
}

func createListener(vmConfig *config.DockerVMConfig) (net.Listener, error) {
	if vmConfig.ConnectionProtocol == config.UDSProtocol {
		sockDir := filepath.Join(vmConfig.DockerVMMountPath, config.RuntimeSockDir)
		runtimeServerSockPath := filepath.Join(sockDir, config.RuntimeSockName)
		err := utils.CreateDir(sockDir)
		if err != nil {
			return nil, err
		}

		return createUnixListener(runtimeServerSockPath)
	}

	// TODO: TLS
	return createTCPListener(strconv.Itoa(vmConfig.RuntimeServer.Port))
}

func createUnixListener(sockPath string) (*net.UnixListener, error) {
	listenAddress, err := net.ResolveUnixAddr("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

start:
	listener, err := net.ListenUnix("unix", listenAddress)
	if err != nil {
		err = os.Remove(sockPath)
		if err != nil {
			return nil, err
		}
		goto start
	}

	if err = os.Chmod(sockPath, 0777); err != nil {
		return nil, err
	}

	return listener, nil
}

func createTCPListener(port string) (*net.TCPListener, error) {
	listenAddress, err := net.ResolveTCPAddr("tcp", ":"+port)
	if err != nil {
		return nil, err
	}

	return net.ListenTCP("tcp", listenAddress)
}
