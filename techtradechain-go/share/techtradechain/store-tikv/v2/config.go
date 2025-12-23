/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package tikvdbprovider

// TiKVDbConfig encapsulated connecton configuration to tikv
type TiKVDbConfig struct {
	Endpoints            string `mapstructure:"endpoints"`
	MaxBatchCount        uint   `mapstructure:"max_batch_count"`
	GrpcConnectionCount  uint   `mapstructure:"grpc_connection_count"`
	GrpcKeepAliveTime    uint   `mapstructure:"grpc_keep_alive_time"`
	GrpcKeepAliveTimeout uint   `mapstructure:"grpc_keep_alive_timeout"`
	WriteBatchSize       uint64 `mapstructure:"write_batch_size"`
	MaxScanLimit         uint64 `mapstructure:"max_scan_limit"`
	ScanBatchSize        uint64 `mapstructure:"scan_batch_size"`
	DbPrefix             string `mapstructure:"db_prefix"`
}
