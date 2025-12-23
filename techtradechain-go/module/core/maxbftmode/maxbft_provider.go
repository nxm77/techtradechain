/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package maxbftmode

import (
	"techtradechain.com/techtradechain-go/module/core/provider"
	"techtradechain.com/techtradechain-go/module/core/provider/conf"
	"techtradechain.com/techtradechain/protocol/v2"
)

const ConsensusTypeMAXBFT = "MAXBFT"

var NilTMAXBFTProvider provider.CoreProvider = (*maxbftProvider)(nil)

type maxbftProvider struct {
}

func (hp *maxbftProvider) NewCoreEngine(config *conf.CoreEngineConfig) (protocol.CoreEngine, error) {
	return NewCoreEngine(config)
}
