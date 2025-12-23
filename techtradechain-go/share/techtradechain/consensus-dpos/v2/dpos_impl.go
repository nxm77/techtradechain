/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dpos

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"techtradechain.com/techtradechain/logger/v2"

	utils "techtradechain.com/techtradechain/consensus-utils/v2"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/pb-go/v2/consensus/dpos"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-native/v2/dposmgr"

	"github.com/gogo/protobuf/proto"
)

var (
	_ protocol.ConsensusState  = (*DPoSImpl)(nil)
	_ protocol.ConsensusEngine = (*DPoSImpl)(nil)
)

// DPoSImpl DPoS consensus algorithm implementation
type DPoSImpl struct {
	// log
	log protocol.Logger

	// consensus engine
	protocol.ConsensusEngine
	// chainconfig
	chainConf protocol.ChainConf
	// state db
	stateDB protocol.BlockchainStore
}

// Start dpos consensus service starts running
func (impl *DPoSImpl) Start() error {
	// invoke base consensus engine
	return impl.ConsensusEngine.Start()
}

// Stop dpos consensus service stopped running
func (impl *DPoSImpl) Stop() error {
	// invoke base consensus engine
	return impl.ConsensusEngine.Stop()
}

// NewDPoSImpl create dpos instance with config and base consensus engine
func NewDPoSImpl(config *utils.ConsensusImplConfig, engine protocol.ConsensusExtendEngine) *DPoSImpl {
	// initialize dpos
	// The dpos log level is independently configured according to the dpos
	log := logger.GetLoggerByChain(logger.MODULE_DPOS, config.ChainConf.ChainConfig().ChainId)
	dposImpl := &DPoSImpl{
		log:       log,
		chainConf: config.ChainConf,
		stateDB:   config.Store,
	}
	// init the dpos into consensus engine
	engine.InitExtendHandler(dposImpl)
	dposImpl.ConsensusEngine = engine
	return dposImpl
}

// SetConsensusEngine set consensus engine
func (impl *DPoSImpl) SetConsensusEngine(engine protocol.ConsensusEngine) {
	impl.ConsensusEngine = engine
}

// CreateRWSet create a dpos read-write set, which will be appended to the Args field of the proposal block
func (impl *DPoSImpl) CreateRWSet(preBlkHash []byte, proposedBlock *consensus.ProposalBlock) error {
	// 1. judge consensus: DPoS
	if !impl.isDPoSConsensus() {
		return nil
	}

	//create a dpos read-write set
	consensusRwSets, err := impl.createDPoSRWSet(preBlkHash, proposedBlock)
	if err != nil {
		return err
	}
	if consensusRwSets != nil {
		// add consensus args to block
		err = impl.addConsensusArgsToBlock(consensusRwSets, proposedBlock.Block)
	}
	return err
}

// createDPoSRWSet create a dpos read-write set
func (impl *DPoSImpl) createDPoSRWSet(
	preBlkHash []byte, proposedBlock *consensus.ProposalBlock) (*common.TxRWSet, error) {
	impl.log.Debugf("begin createDPoS rwSet, blockInfo: %d:%x ",
		proposedBlock.Block.Header.BlockHeight, proposedBlock.Block.Header.BlockHash)
	var (
		block        = proposedBlock.Block
		blockTxRwSet = proposedBlock.TxsRwSet
	)
	blockHeight := uint64(block.Header.BlockHeight)
	// 2. get epoch info from stateDB
	epoch, err := impl.getEpochInfo()
	if err != nil {
		return nil, err
	}
	if epoch.NextEpochCreateHeight != blockHeight {
		return nil, nil
	}
	// 3. create unbounding rwset
	unboundingRwSet, err := impl.completeUnbounding(epoch, block, blockTxRwSet)
	if err != nil {
		impl.log.Errorf("create complete unbonding error, reason: %s", err)
		return nil, err
	}
	// 4. create newEpoch
	newEpoch, err := impl.createNewEpoch(blockHeight, epoch, preBlkHash)
	if err != nil {
		impl.log.Errorf("create new epoch error, reason: %s", err)
		return nil, err
	}
	//create epoch RwSet
	epochRwSet, err := impl.createEpochRwSet(newEpoch)
	if err != nil {
		impl.log.Errorf("create epoch rwSet error, reason: %s", err)
		return nil, err
	}
	//create validators RwSet
	validatorsRwSet, err := impl.createValidatorsRwSet(newEpoch)
	if err != nil {
		impl.log.Errorf("create validators rwSet error, reason: %s", err)
		return nil, err
	}
	// 5. Aggregate read-write set
	unboundingRwSet.TxWrites = append(unboundingRwSet.TxWrites, epochRwSet.TxWrites...)
	unboundingRwSet.TxWrites = append(unboundingRwSet.TxWrites, validatorsRwSet.TxWrites...)
	impl.log.Debugf("end createDPoS rwSet: %v ", unboundingRwSet)
	return unboundingRwSet, nil
}

// isDPoSConsensus read the blockchain configuration, if dpos returns true, otherwise return false
func (impl *DPoSImpl) isDPoSConsensus() bool {
	// read the blockchain configuration and return different
	// results according to the consensus type in the configuration
	return impl.chainConf.ChainConfig().Consensus.Type == consensus.ConsensusType_DPOS
}

// createNewEpoch create new epoch
func (impl *DPoSImpl) createNewEpoch(
	proposalHeight uint64, oldEpoch *syscontract.Epoch, seed []byte) (*syscontract.Epoch, error) {
	impl.log.Debugf("begin create new epoch in blockHeight: %d", proposalHeight)
	// 1. get property: epochBlockNum
	epochBlockNumBz, err := impl.stateDB.ReadObject(
		syscontract.SystemContract_DPOS_STAKE.String(), []byte(dposmgr.KeyEpochBlockNumber))
	if err != nil {
		impl.log.Errorf("load epochBlockNum from db failed, reason: %s", err)
		return nil, err
	}
	epochBlockNum := binary.BigEndian.Uint64(epochBlockNumBz)
	impl.log.Debugf("epoch blockNum: %d", epochBlockNum)

	// 2. get all candidates
	candidates, err := impl.getAllCandidateInfo()
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		impl.log.Errorf("not found candidates from contract")
		return nil, fmt.Errorf("not found candidates from contract")
	}

	// 3. select validators from candidates
	validators, err := impl.selectValidators(candidates, seed)
	if err != nil {
		return nil, err
	}
	proposer := make([]string, 0, len(validators))
	for _, val := range validators {
		proposer = append(proposer, val.PeerId)
	}

	// 4. create NewEpoch
	newEpoch := &syscontract.Epoch{
		EpochId:               oldEpoch.EpochId + 1,
		NextEpochCreateHeight: proposalHeight + epochBlockNum,
		ProposerVector:        proposer,
	}
	impl.log.Debugf("new epoch: %s", newEpoch.String())
	return newEpoch, nil
}

// selectValidators select validators before the epoch switch
func (impl *DPoSImpl) selectValidators(candidates []*dpos.CandidateInfo, seed []byte) ([]*dpos.CandidateInfo, error) {
	//returns the state value for given contract name and key, or returns nil if none exists.
	valNumBz, err := impl.stateDB.ReadObject(
		syscontract.SystemContract_DPOS_STAKE.String(), []byte(dposmgr.KeyEpochValidatorNumber))
	if err != nil {
		impl.log.Errorf("load epochBlockNum from db failed, reason: %s", err)
		return nil, err
	}
	valNum := binary.BigEndian.Uint64(valNumBz)
	vals, err := ValidatorsElection(candidates, int(valNum), seed, true)
	if err != nil {
		impl.log.Errorf("select validators from candidates failed, reason: %s", err)
		return nil, err
	}
	impl.log.Debugf("select validators: %v from candidates: %v by seed: %x", vals, candidates, seed)
	return vals, nil
}

// addConsensusArgsToBlock add consensus args to block
func (impl *DPoSImpl) addConsensusArgsToBlock(rwSet *common.TxRWSet, block *common.Block) error {
	impl.log.Debugf("begin add consensus args to block ")
	if !impl.isDPoSConsensus() {
		return nil
	}
	consensusArgs := &consensus.BlockHeaderConsensusArgs{
		ConsensusType: int64(consensus.ConsensusType_DPOS),
		ConsensusData: rwSet,
	}
	argBytes, err := proto.Marshal(consensusArgs)
	if err != nil {
		impl.log.Errorf("marshal BlockHeaderConsensusArgs failed, reason: %s", err)
		return err
	}
	block.Header.ConsensusArgs = argBytes
	impl.log.Debugf("end add consensus args ")
	return nil
}

// VerifyConsensusArgs verify consensus args from the slave node
func (impl *DPoSImpl) VerifyConsensusArgs(block *common.Block, blockTxRwSet map[string]*common.TxRWSet) (err error) {
	impl.log.Debugf(
		"begin VerifyConsensusArgs, blockHeight: %d, blockHash: %x",
		block.Header.BlockHeight, block.Header.BlockHash)
	if !impl.isDPoSConsensus() {
		return nil
	}

	// create a dpos read-write set
	localConsensus, err := impl.createDPoSRWSet(
		block.Header.PreBlockHash, &consensus.ProposalBlock{Block: block, TxsRwSet: blockTxRwSet})
	if err != nil {
		impl.log.Errorf("get DPoS txRwSets failed, reason: %s", err)
		return err
	}

	var localBz []byte
	if localConsensus != nil {
		localBz, err = proto.Marshal(&consensus.BlockHeaderConsensusArgs{
			ConsensusType: int64(consensus.ConsensusType_DPOS),
			ConsensusData: localConsensus,
		})
		if err != nil {
			impl.log.Errorf("marshal BlockHeaderConsensusArgs failed, reason: %s", err)
			return err
		}
	}
	// the same is verified
	if bytes.Equal(block.Header.ConsensusArgs, localBz) {
		return nil
	}
	// if they are not the same, the verification fails, and a believe error message is returned.
	consensusArgs := &consensus.BlockHeaderConsensusArgs{}
	if err = proto.Unmarshal(block.Header.ConsensusArgs, consensusArgs); err != nil {
		return fmt.Errorf("unmarshal dpos consensusArgs from blockHeader failed,reason: %s ", err)
	}
	return fmt.Errorf("consensus args verify mismatch, blockConsensus: %v, "+
		"localConsensus: %v by seed: %x", consensusArgs, localConsensus, block.Header.PreBlockHash)
}

// GetValidators take the validator list from the epoch
func (impl *DPoSImpl) GetValidators() ([]string, error) {
	if !impl.isDPoSConsensus() {
		return nil, nil
	}
	//get epoch info from ledger
	epoch, err := impl.getEpochInfo()
	if err != nil {
		return nil, err
	}
	//get node IDs from validators
	nodeIDs, err := impl.getNodeIDsFromValidators(epoch)
	return nodeIDs, err
}

// GetLastHeight get the last block height
func (impl *DPoSImpl) GetLastHeight() uint64 {
	v, _ := impl.ConsensusEngine.(protocol.ConsensusState)
	return v.GetLastHeight()
}

// DPoSState DPoS status information
type DPoSState struct {
	// epoch information
	EpochInfo *syscontract.Epoch
	// candidate information
	CandidateInfos []NodeInfo
	// internal consensus state
	InternalConsensusState []byte
}

// NodeInfo node information
type NodeInfo struct {
	// node id
	NodeId string
	// stake weight
	StakeWeight string
	// candidate address
	CandidateAddr string
}

// GetConsensusStateJSON convert consensus state to json
func (impl *DPoSImpl) GetConsensusStateJSON() ([]byte, error) {
	// get epoch info from ledger
	epoch, err := impl.getEpochInfo()
	if err != nil {
		return nil, err
	}
	// get all candidates from ledger
	infos, err := impl.getAllCandidateInfo()
	if err != nil {
		return nil, err
	}
	candidateAddrs := make([]string, 0, len(infos))
	candidateInfos := make([]NodeInfo, 0, len(infos))
	for _, info := range infos {
		candidateAddrs = append(candidateAddrs, info.PeerId)
	}
	// query the corresponding node id of the validator from the ledger,
	// and return the list of node ids
	nodeIds, err := getNodeIDsFromValidators(impl.stateDB, candidateAddrs)
	if err != nil {
		return nil, err
	}
	for i, nodeId := range nodeIds {
		candidateInfos = append(candidateInfos, NodeInfo{
			NodeId:        nodeId,
			StakeWeight:   infos[i].Weight,
			CandidateAddr: infos[i].PeerId,
		})
	}
	internalConsensusEngine, _ := impl.ConsensusEngine.(protocol.ConsensusState)
	//convert consensus state to json
	internalState, err := internalConsensusEngine.GetConsensusStateJSON()
	if err != nil {
		return nil, err
	}

	return json.Marshal(DPoSState{
		EpochInfo:              epoch,
		CandidateInfos:         candidateInfos,
		InternalConsensusState: internalState,
	})
}
