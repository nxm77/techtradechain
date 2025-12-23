/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package consensus

import (
	"fmt"

	maxbft "techtradechain.com/techtradechain/consensus-maxbft/v2"

	dpos "techtradechain.com/techtradechain/consensus-dpos/v2"
	raft "techtradechain.com/techtradechain/consensus-raft/v2"
	tbft "techtradechain.com/techtradechain/consensus-tbft/v2"
	commonpb "techtradechain.com/techtradechain/pb-go/v2/common"
	consensuspb "techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/protocol/v2"
)

// VerifyBlockSignatures verifies whether the signatures in block
// is qulified with the consensus algorithm. It should return nil
// error when verify successfully, and return corresponding error
// when failed.
func VerifyBlockSignatures(
	chainConf protocol.ChainConf,
	ac protocol.AccessControlProvider,
	store protocol.BlockchainStore,
	block *commonpb.Block,
	ledger protocol.LedgerCache,
) error {
	consensusType := chainConf.ChainConfig().Consensus.Type
	switch consensusType {
	case consensuspb.ConsensusType_TBFT:
		// get validator list by module of tbft
		return tbft.VerifyBlockSignatures(chainConf, ac, block, store, tbft.GetValidatorList)
	case consensuspb.ConsensusType_DPOS:
		// get validator list by module of dpos
		return tbft.VerifyBlockSignatures(chainConf, ac, block, store, dpos.GetValidatorList)
	case consensuspb.ConsensusType_RAFT:
		return raft.VerifyBlockSignatures(block)
	case consensuspb.ConsensusType_MAXBFT:
		return maxbft.VerifyBlockSignatures(chainConf, ac, store, block, ledger)
	case consensuspb.ConsensusType_SOLO:
		return nil //for rebuild-dbs
	default:
	}
	return fmt.Errorf("error consensusType: %s", consensusType)
}
