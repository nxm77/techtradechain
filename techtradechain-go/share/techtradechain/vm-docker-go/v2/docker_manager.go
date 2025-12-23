/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package docker_go

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/protocol/v2"

	"github.com/mitchellh/mapstructure"

	"techtradechain.com/techtradechain/vm-docker-go/v2/rpc"

	"techtradechain.com/techtradechain/logger/v2"
	"techtradechain.com/techtradechain/vm-docker-go/v2/config"
)

type DockerManager struct {
	chainId               string
	mgrLogger             *logger.CMLogger
	ctx                   context.Context
	clientManager         *rpc.ClientManager            // grpc client
	dockerVMConfig        *config.DockerVMConfig        // original config from local config
	dockerContainerConfig *config.DockerContainerConfig // container setting
	RequestMgr            *RequestMgr
}

// NewDockerManager return docker manager and running a default container
func NewDockerManager(chainId string, vmConfig map[string]interface{}) protocol.VmInstancesManager {

	dockerVMConfig := &config.DockerVMConfig{}
	_ = mapstructure.Decode(vmConfig, dockerVMConfig)

	// if enable docker vm is false, docker manager is nil
	startDockerVm := dockerVMConfig.EnableDockerVM
	if !startDockerVm {
		return nil
	}

	// init docker manager logger
	dockerManagerLogger := logger.GetLoggerByChain("[Docker Manager]", chainId)
	dockerManagerLogger.Debugf("init docker manager")

	// validate and init settings
	dockerContainerConfig := newDockerContainerConfig()
	err := validateVMSettings(dockerVMConfig, dockerContainerConfig, chainId)
	if err != nil {
		dockerManagerLogger.Errorf("fail to init docker manager, please check the docker config, %s", err)
		return nil
	}

	// init docker manager
	newDockerManager := &DockerManager{
		chainId:               chainId,
		mgrLogger:             dockerManagerLogger,
		ctx:                   context.Background(),
		clientManager:         rpc.NewClientManager(dockerVMConfig),
		dockerVMConfig:        dockerVMConfig,
		dockerContainerConfig: dockerContainerConfig,
		RequestMgr:            NewRequestMgr(),
	}

	// init mount directory and subdirectory
	err = newDockerManager.initMountDirectory()
	if err != nil {
		dockerManagerLogger.Errorf("fail to init mount directory: %s", err)
		return nil
	}

	return newDockerManager
}

// StartVM Start Docker VM
func (m *DockerManager) StartVM() error {
	if m == nil {
		return nil
	}
	m.mgrLogger.Info("start docker vm...")
	// todo verify vm contract service info

	return m.clientManager.Start()
}

// StopVM stop docker vm and remove container, image
func (m *DockerManager) StopVM() error {
	if m == nil {
		return nil
	}
	// close all clients
	m.clientManager.CloseAllConnections()
	return nil
}

func (m *DockerManager) BeforeSchedule(blockFingerprint string, blockHeight uint64) {
	m.RequestMgr.AddRequest(blockFingerprint)
}

func (m *DockerManager) AfterSchedule(blockFingerprint string, blockHeight uint64) {
	m.mgrLogger.InfoDynamic(
		func() string {
			return fmt.Sprintf("BlockHeight: %d, %s", blockHeight, m.RequestMgr.PrintBlockElapsedTime(blockFingerprint))
		})
	m.RequestMgr.RemoveRequest(blockFingerprint)
}

func (m *DockerManager) NewRuntimeInstance(txSimContext protocol.TxSimContext, chainId, method,
	codePath string, contract *common.Contract,
	byteCode []byte, logger protocol.Logger) (protocol.RuntimeInstance, error) {

	return &RuntimeInstance{
		ChainId:       chainId,
		ClientManager: m.clientManager,
		Log:           logger,
		DockerManager: m,
	}, nil
}

// InitMountDirectory init mount directory and subdirectories
func (m *DockerManager) initMountDirectory() error {

	var err error

	// create mount directory
	mountDir := m.dockerContainerConfig.HostMountDir
	err = m.createDir(mountDir)
	if err != nil {
		return err
	}
	m.mgrLogger.Debug("set mount dir: ", mountDir)

	// create subdirectory: contracts
	contractDir := filepath.Join(mountDir, config.ContractsDir)
	err = m.createDir(contractDir)
	if err != nil {
		return err
	}
	m.mgrLogger.Debug("set contract dir: ", contractDir)

	// create log directory
	logDir := m.dockerContainerConfig.HostLogDir
	err = m.createDir(logDir)
	if err != nil {
		return nil
	}
	m.mgrLogger.Debug("set log dir: ", logDir)

	return nil

}

// ------------------ utility functions --------------

func (m *DockerManager) createDir(directory string) error {
	exist, err := m.exists(directory)
	if err != nil {
		m.mgrLogger.Errorf("fail to get container, err: [%s]", err)
		return err
	}

	if !exist {
		err = os.MkdirAll(directory, 0755)
		if err != nil {
			m.mgrLogger.Errorf("fail to remove image, err: [%s]", err)
			return err
		}
	}

	return nil
}

// exists returns whether the given file or directory exists
func (m *DockerManager) exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func validateVMSettings(config *config.DockerVMConfig,
	dockerContainerConfig *config.DockerContainerConfig, chainId string) error {

	var hostMountDir string
	var hostLogDir string
	if len(config.DockerVMMountPath) == 0 {
		return errors.New("doesn't set host mount directory path correctly")
	}

	if len(config.DockerVMLogPath) == 0 {
		return errors.New("doesn't set host log directory path correctly")
	}

	// set host mount directory path
	if !filepath.IsAbs(config.DockerVMMountPath) {
		hostMountDir, _ = filepath.Abs(config.DockerVMMountPath)
		hostMountDir = filepath.Join(hostMountDir, chainId)
	} else {
		hostMountDir = filepath.Join(config.DockerVMMountPath, chainId)
	}

	// set host log directory
	hostLogDir, _ = filepath.Abs(config.DockerVMLogPath)

	dockerContainerConfig.HostMountDir = hostMountDir
	dockerContainerConfig.HostLogDir = hostLogDir

	return nil
}

func newDockerContainerConfig() *config.DockerContainerConfig {

	containerConfig := &config.DockerContainerConfig{
		HostMountDir: "",
		HostLogDir:   "",
	}

	return containerConfig
}
