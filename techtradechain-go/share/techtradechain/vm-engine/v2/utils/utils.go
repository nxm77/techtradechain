package utils

import (
	"fmt"
	"os"
	"strings"

	"techtradechain.com/techtradechain/vm-engine/v2/config"
)

const (
	// DefaultMaxConnection is default max connection number
	DefaultMaxConnection = 1
)

// SplitContractName split contract name
func SplitContractName(contractNameAndVersion string) string {
	contractName := strings.Split(contractNameAndVersion, "#")[0]
	return contractName
}

// GetMaxSendMsgSizeFromConfig returns max send msg size from config
func GetMaxSendMsgSizeFromConfig(conf *config.DockerVMConfig) uint32 {
	if conf.MaxSendMsgSize < config.DefaultMaxSendSize {
		return config.DefaultMaxSendSize
	}
	return conf.MaxSendMsgSize
}

// GetMaxRecvMsgSizeFromConfig returns max recv msg size from config
func GetMaxRecvMsgSizeFromConfig(conf *config.DockerVMConfig) uint32 {
	if conf.MaxRecvMsgSize < config.DefaultMaxRecvSize {
		return config.DefaultMaxRecvSize
	}
	return conf.MaxRecvMsgSize
}

// GetMaxConnectionFromConfig returns max connections from config
func GetMaxConnectionFromConfig(config *config.DockerVMConfig) uint32 {
	if config.ContractEngine.MaxConnection == 0 {
		return DefaultMaxConnection
	}
	return uint32(config.ContractEngine.MaxConnection) //nolint:gosec
}

// ConstructNotifyMapKey chainId#txId
func ConstructNotifyMapKey(names ...string) string {
	return strings.Join(names, "#")
}

// ConstructUniqueTxKey txId#count
func ConstructUniqueTxKey(names ...string) string {
	return strings.Join(names, "#")
}

// CreateDir create dir
func CreateDir(directory string) error {
	exist, err := Exists(directory)
	if err != nil {
		return err
	}

	if !exist {
		err = os.MkdirAll(directory, 0755)
		if err != nil {
			return fmt.Errorf("failed to create [%s], err: [%s]", directory, err)
		}
	}

	return nil
}

// Exists returns whether the given file or directory exists
func Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
