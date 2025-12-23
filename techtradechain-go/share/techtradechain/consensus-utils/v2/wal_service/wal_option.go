/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package wal_service

// WalWriteMode the mode of wal write
type WalWriteMode int

const (
	// SyncWalWrite sync wal write
	SyncWalWrite WalWriteMode = iota // write wal sync: 0
	// AsyncWalWrite async wal write
	AsyncWalWrite
	// NonWalWrite none wal write
	NonWalWrite
	// WALWriteModeKey mode key
	WALWriteModeKey = "WAL_write_mode"
	// WalDir wal dir
	WalDir = "wal"
)

// ConsensusWalOptionFunc option func
type ConsensusWalOptionFunc func(option *ConsensusWalOption)

// MarshalFunc the function which marshal data to bytes
// if there is error when marshal, the process will panic
type MarshalFunc func(data interface{}) []byte

// ConsensusWalOption wal option struct
type ConsensusWalOption struct {
	walWriteMode WalWriteMode
	walWritePath string
}

//NewDefaultConsensusWalOption walWriteMode
func NewDefaultConsensusWalOption() ConsensusWalOption {
	return ConsensusWalOption{
		walWriteMode: NonWalWrite, // default non write wal
	}
}

//WithWriteMode add WithWriteMode to option
func WithWriteMode(walWriteMode WalWriteMode) ConsensusWalOptionFunc {
	return func(option *ConsensusWalOption) {
		option.walWriteMode = walWriteMode
	}
}

//WithWritePath add walWritePath to option
func WithWritePath(walWritePath string) ConsensusWalOptionFunc {
	return func(option *ConsensusWalOption) {
		option.walWritePath = walWritePath
	}
}
