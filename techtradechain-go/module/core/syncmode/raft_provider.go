/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package syncmode

import (
	"techtradechain.com/techtradechain-go/module/core/provider"
	"techtradechain.com/techtradechain-go/module/core/provider/conf"
	"techtradechain.com/techtradechain/protocol/v2"
)

const ConsensusTypeRAFT = "RAFT"

var NilRAFTProvider provider.CoreProvider = (*raftProvider)(nil)

type raftProvider struct {
}

func (rp *raftProvider) NewCoreEngine(config *conf.CoreEngineConfig) (protocol.CoreEngine, error) {
	return NewCoreEngine(config)
}
