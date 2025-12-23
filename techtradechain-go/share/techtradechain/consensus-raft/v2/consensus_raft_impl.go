/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package raft is the raft consensus for TechTradeChain
package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"techtradechain.com/techtradechain/chainconf/v2"
	commonErrors "techtradechain.com/techtradechain/common/v2/errors"
	"techtradechain.com/techtradechain/common/v2/msgbus"
	consensusUtils "techtradechain.com/techtradechain/consensus-utils/v2"
	"techtradechain.com/techtradechain/localconf/v2"
	"techtradechain.com/techtradechain/logger/v2"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/consensus"
	consensuspb "techtradechain.com/techtradechain/pb-go/v2/consensus"
	netpb "techtradechain.com/techtradechain/pb-go/v2/net"
	systemPb "techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/raftwal/v2/wal"
	"techtradechain.com/techtradechain/utils/v2"

	"github.com/gogo/protobuf/proto"
	"github.com/thoas/go-funk"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	etcdraft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
)

var (
	// DefaultChanCap is the default chan cap in ConsensusRaftImpl
	DefaultChanCap = 1000

	// walDir is the default wal directory
	walDir = "raftwal"

	// snapDir is the default snapshot directory
	snapDir = "snap"

	snapshotCatchUpEntriesN = uint64(5)
	defaultSnapCount        = uint64(10)
	defaultElectionTick     = 10

	// RAFTAddtionalDataKey is the key for QC
	RAFTAddtionalDataKey = "RAFTAddtionalDataKey"

	isStarted sync.Map
	instances sync.Map

	// ErrNotConsensusNode is an error tells that the current node
	// is a sync node and it not support consensus state interface
	ErrNotConsensusNode = errors.New("the node is not a consensus node")
)

// mustMarshal marshals protobuf message to byte slice or panic
func mustMarshal(msg proto.Message) []byte {
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return data
}

// mustUnmarshal unmarshals from byte slice to protobuf message or panic
func mustUnmarshal(b []byte, msg proto.Message) {
	if err := proto.Unmarshal(b, msg); err != nil {
		panic(err)
	}
}

// ConsensusRaftImpl is the implementation of Raft algorithm
// and it implements the ConsensusEngine interface.
type ConsensusRaftImpl struct {
	sync.RWMutex
	logger protocol.Logger
	// chainID from the blockChain
	chainID string

	// signer used to sign the blocks
	singer protocol.SigningMember

	// the access controller module interface
	ac protocol.AccessControlProvider

	// the ledger cache of the blockChain
	ledgerCache protocol.LedgerCache

	// the current chain configurations
	chainConf protocol.ChainConf

	// message bus used to interact with other modules
	// (core engine and net module)
	msgbus msgbus.MessageBus

	// closeC used to close all goroutines of the consensus module
	closeC chan struct{}

	// the nodeId of blockChain
	nodeId string

	// the nodeId of etcd-raft
	Id uint64

	// the current consensus nodeIds of etcd-raft
	peers []uint64

	// the local node is the leader now in the blockChain
	isLeader bool

	// the etcd node object
	node etcdraft.Node

	// the etcd memory storage, used to maintain log entries in consensus
	raftStorage *etcdraft.MemoryStorage

	// used to save and replay wal when restarted
	wal          *wal.WAL
	waldir       string
	asyncWalSave bool

	// used to save and load snapshots
	snapdir       string
	snapCount     uint64
	snapshotter   *snap.Snapshotter
	confState     raftpb.ConfState
	snapshotIndex uint64

	// current proposer's nodeId
	Proposer string

	// some state information of the consensus module
	appliedIndex  uint64
	lastHeight    uint64
	proposedIndex uint64

	hasReportSnap bool

	electionTick int
	raftKey      string
	idToNodeId   sync.Map

	proposedBlockC chan *common.Block
	verifyResultC  chan *consensus.VerifyResult
	blockInfoC     chan *common.BlockInfo
	confChangeC    chan raftpb.ConfChange
	walSaveC       chan interface{}
	wg             sync.WaitGroup

	// verifier of the core engine
	blockVerifier protocol.BlockVerifier

	// committer of the core engine
	blockCommitter protocol.BlockCommitter

	// for ConsensusState interface
	blockStorage   protocol.BlockchainStore
	stateMutex     sync.Mutex
	lastConfHeight uint64
	isValidator    bool
}

// SnapshotArgs is snapshot args for raft
type SnapshotArgs struct {
	Index       uint64
	BlockHeight uint64
	ConfChange  bool
}

// New creates a raft consensus instance
func New(config *consensusUtils.ConsensusImplConfig) (*ConsensusRaftImpl, error) {
	consensus := &ConsensusRaftImpl{}
	consensus.Id = computeRaftIdFromNodeId(config.NodeId)
	lg := config.Logger
	consensus.raftKey = fmt.Sprintf("%s_%s", config.NodeId, config.ChainId)

	// to avoid that there are multiple consensus engines running at the same time
	if started, ok := isStarted.Load(consensus.raftKey); ok && started.(bool) {
		if ins, ok := instances.Load(consensus.raftKey); ok && ins.(*ConsensusRaftImpl) != nil {
			lg.Infof("ConsensusRaftImpl[%x] is already exist, need to do nothing. raftKey:%s",
				consensus.Id, consensus.raftKey)
			return ins.(*ConsensusRaftImpl), nil
		}
		isStarted.Delete(consensus.raftKey)
	}

	consensus.logger = lg
	consensus.chainID = config.ChainId
	consensus.singer = config.Signer
	consensus.ac = config.Ac
	consensus.ledgerCache = config.LedgerCache
	consensus.chainConf = config.ChainConf
	consensus.msgbus = config.MsgBus
	consensus.closeC = make(chan struct{})

	consensus.snapCount = localconf.TechTradeChainConfig.ConsensusConfig.RaftConfig.SnapCount
	if consensus.snapCount == 0 {
		consensus.snapCount = defaultSnapCount
	}
	consensus.asyncWalSave = localconf.TechTradeChainConfig.ConsensusConfig.RaftConfig.AsyncWalSave
	consensus.waldir = path.Join(localconf.TechTradeChainConfig.GetStorePath(), consensus.chainID,
		fmt.Sprintf("%s_%s", walDir, config.NodeId))
	consensus.snapdir = path.Join(localconf.TechTradeChainConfig.GetStorePath(), consensus.chainID,
		fmt.Sprintf("%s_%s", snapDir, config.NodeId))

	consensus.proposedBlockC = make(chan *common.Block, DefaultChanCap)
	consensus.verifyResultC = make(chan *consensuspb.VerifyResult, DefaultChanCap)
	consensus.blockInfoC = make(chan *common.BlockInfo, DefaultChanCap)
	consensus.confChangeC = make(chan raftpb.ConfChange, DefaultChanCap)
	consensus.walSaveC = make(chan interface{}, DefaultChanCap)
	consensus.blockVerifier = config.Core.GetBlockVerifier()
	consensus.blockCommitter = config.Core.GetBlockCommitter()
	consensus.blockStorage = config.Store

	consensus.logger.Infof("New ConsensusRaftImpl[%x]", consensus.Id)
	instances.Store(consensus.raftKey, consensus)
	consensus.electionTick = defaultElectionTick
	return consensus, nil
}

// Start starts the raft instance
func (consensus *ConsensusRaftImpl) Start() error {
	if started, ok := isStarted.Load(consensus.raftKey); ok && started.(bool) {
		consensus.logger.Infof("ConsensusRaftImpl[%x] is already started, need to do nothing", consensus.Id)
		return nil
	}
	consensus.logger.Infof("ConsensusRaftImpl[%x] starting", consensus.Id)
	// create the directory and the file for snapshots when started first time
	if !fileutil.Exist(consensus.snapdir) {
		if err := os.Mkdir(consensus.snapdir, 0750); err != nil {
			consensus.logger.Fatalf("[%x] cannot create dir for snapshot: %v", consensus.Id, err)
			return err
		}
	}
	// snapshotter is used to load and save snapshots
	consensus.snapshotter = snap.New(consensus.logger.(*logger.CMLogger).Logger().Desugar(), consensus.snapdir)
	walExist := wal.Exist(consensus.waldir)
	// replay the wal files
	consensus.wal = consensus.replayWAL()

	var idToNodes map[uint64]string
	consensus.peers, idToNodes = consensus.getPeersFromChainConf()
	for id, node := range idToNodes {
		consensus.idToNodeId.Store(id, node)
	}
	c := &etcdraft.Config{
		ID:              consensus.Id,
		ElectionTick:    consensus.electionTick,
		HeartbeatTick:   1,
		Storage:         consensus.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		// CheckQuorum:     true,
		Logger: NewLogger(consensus.logger.(*logger.CMLogger).Logger()),
	}

	height, err := consensus.ledgerCache.CurrentHeight()
	if err != nil {
		return err
	}
	consensus.lastHeight = height

	if walExist || height != 0 {
		consensus.logger.Infof("[%x] restart raft walExist: %v, height: %v", consensus.Id, walExist, height)
		// this is not the first time to start, construct and
		// start the etcd-raft Node without consensus nodeIds,
		// the etcd-raft will load them from the snapshot
		consensus.node = etcdraft.RestartNode(c)
	} else {
		consensus.logger.Infof("[%x] start raft walExist: %v, height: %v", consensus.Id, walExist, height)
		peers := []etcdraft.Peer{}
		for _, p := range consensus.peers {
			peers = append(peers, etcdraft.Peer{ID: p})
		}
		// construct and start the etcd-raft Node with the current
		// consensus nodeIds, when started in the first time
		consensus.node = etcdraft.StartNode(c, peers)
	}
	consensus.wg = sync.WaitGroup{}
	consensus.wg.Add(1)
	// start processing wal asynchronously
	go consensus.AsyncWalSave()
	// start serving
	go consensus.serve()
	consensus.msgbus.Register(msgbus.ProposedBlock, consensus)
	consensus.msgbus.Register(msgbus.RecvConsensusMsg, consensus)
	_ = chainconf.RegisterVerifier(consensus.chainID, consensuspb.ConsensusType_RAFT, consensus)
	isStarted.Store(consensus.raftKey, true)

	return nil
}

// Stop stops the raft instance, we have to do nothing
// in here because we can't stopped by the blockChain
// directly, we should tell etcd-raft that the current
// node was removed by some configuration changing
// before exit
func (consensus *ConsensusRaftImpl) Stop() error {
	consensus.logger.Infof("ConsensusRaftImpl stopping")
	return nil
}

// OnMessage receives messages from msgbus
func (consensus *ConsensusRaftImpl) OnMessage(message *msgbus.Message) {
	switch message.Topic {
	// receive a proposed block from the core engine
	case msgbus.ProposedBlock:
		if proposedBlock, ok := message.Payload.(*consensuspb.ProposalBlock); ok {
			// pass the block to the serving loop to process
			consensus.proposedBlockC <- proposedBlock.Block
		} else {
			consensus.logger.Errorf("receive ProposalBlock message failed, error message type")
			return
		}
	// receive a etcd-raft Message from another node
	case msgbus.RecvConsensusMsg:
		if msg, ok := message.Payload.(*netpb.NetMsg); ok {
			raftMsg := raftpb.Message{}
			// unmarshal the etcd-raft Message
			if err := raftMsg.Unmarshal(msg.Payload); err != nil {
				consensus.logger.Errorf("[%x] unmarshal message %v", consensus.Id, err)
				return
			}

			// if the leader got a raftpb.MsgSnapStatus message,
			// it proves that the snapshot was successfully or failure
			// applied to the follower, and we should report this to
			// the etcd raft-engine to change the state of follower
			// in raft-engine's prs
			//
			// The RejectHint indicates the block height of the follower,
			// if the block height is bigger than pr.PendingSnapshot, the
			// pending snapshot must be applied successfully.
			// We add this to avoid that the last snapshot we have sent
			// to the follower is missing, so that the follower would never
			// report the missing snapshot's result.
			if raftMsg.Type == raftpb.MsgSnapStatus && consensus.isLeader {
				consensus.logger.Debugf("[%x] receive message MsgSnapStatus from %x, index: %d, failure: %v",
					consensus.Id, raftMsg.From, raftMsg.Index, raftMsg.Reject)
				if pr, ok := consensus.node.Status().Progress[raftMsg.From]; ok &&
					(raftMsg.Index == pr.PendingSnapshot || raftMsg.RejectHint > pr.PendingSnapshot) &&
					!raftMsg.Reject {
					consensus.node.ReportSnapshot(raftMsg.From, etcdraft.SnapshotFinish)
				} else if raftMsg.Reject {
					consensus.node.ReportSnapshot(raftMsg.From, etcdraft.SnapshotFailure)
				}
				return
			}

			consensus.logger.DebugDynamic(func() string {
				return fmt.Sprintf("[%x] receive message %v", consensus.Id, describeMessage(raftMsg))
			})
			// pass the Message to the etcd-raft
			if err := consensus.node.Step(context.Background(), raftMsg); err != nil {
				consensus.logger.Errorf("[%x] step message %v, err: %v", consensus.Id, describeMessage(raftMsg), err)
				return
			}
		} else {
			consensus.logger.Errorf("receive RecvConsensusMsg message failed, error message type")
			return
		}
	}
}

// OnQuit is on quit for msgbus
func (consensus *ConsensusRaftImpl) OnQuit() {
	// do nothing
}

// GetValidators returns all validators for ConsensusRaftImpl
func (consensus *ConsensusRaftImpl) GetValidators() ([]string, error) {
	consensus.stateMutex.Lock()
	defer consensus.stateMutex.Unlock()
	return consensus.getValidators()
}

func (consensus *ConsensusRaftImpl) getValidators() ([]string, error) {
	if !consensus.IsValidator() {
		return nil, ErrNotConsensusNode
	}
	var (
		ok     bool
		nodeId string
		node   interface{}
		nodes  = make([]string, 0)
	)
	for _, peer := range consensus.peers {
		if node, ok = consensus.idToNodeId.Load(peer); !ok {
			return nil, fmt.Errorf("unknown peer")
		}
		if nodeId, ok = node.(string); !ok {
			return nil, fmt.Errorf("wrong type NodeId")
		}
		nodes = append(nodes, nodeId)
	}
	return nodes, nil
}

// GetLastHeight returns last height in ConsensusRaftImpl
func (consensus *ConsensusRaftImpl) GetLastHeight() uint64 {
	consensus.stateMutex.Lock()
	defer consensus.stateMutex.Unlock()
	if !consensus.IsValidator() {
		return math.MaxUint64
	}
	return consensus.lastHeight
}

// GetConsensusStateJSON returns the node status
func (consensus *ConsensusRaftImpl) GetConsensusStateJSON() ([]byte, error) {
	consensus.stateMutex.Lock()
	defer consensus.stateMutex.Unlock()
	if !consensus.IsValidator() {
		return nil, ErrNotConsensusNode
	}
	return consensus.node.Status().MarshalJSON()
}

func (consensus *ConsensusRaftImpl) getChainConfigFromChainStore() (*config.ChainConfig, error) {
	contractName := systemPb.SystemContract_CHAIN_CONFIG.String()
	bz, err := consensus.blockStorage.ReadObject(contractName, []byte(contractName))
	if err != nil {
		return nil, err
	}
	var chainConfig config.ChainConfig
	if err = proto.Unmarshal(bz, &chainConfig); err != nil {
		return nil, err
	}
	return &chainConfig, nil
}

// IsValidator checks that the current node is a consensus node or not
func (consensus *ConsensusRaftImpl) IsValidator() bool {
	lastHeight, err := consensus.ledgerCache.CurrentHeight()
	if err != nil {
		consensus.logger.Warnf("get CurrentHeight from ledgerCache failed. error:%+v", err)
		return false
	}
	// if the last block in chain storage is committed by
	// the consensus engine, then the local node is a validator,
	// this is to avoid a validator being attacked by a large
	// number of ConsensusState requests.
	if lastHeight == consensus.lastHeight {
		return true
	}
	// if we have not committed any blocks after the last check,
	// we can return the result of the last check directly
	if lastHeight == consensus.lastConfHeight {
		return consensus.isValidator
	}
	// get chain config from the chain storage, to check if the local node is validator now
	conf, err := consensus.getChainConfigFromChainStore()
	if err != nil {
		consensus.logger.Warnf("get chainConfig from chain storage failed. error:%+v", err)
		return false
	}
	consensus.lastConfHeight = lastHeight
	for _, node := range conf.Consensus.GetNodes() {
		for _, id := range node.NodeId {
			if id == consensus.nodeId {
				consensus.isValidator = true
				return true
			}
		}
	}
	consensus.isValidator = false
	return false
}

// saveSnap saves snapshot information in the local file,
// the snapshot maybe from etcd-raft ready(received from the leader),
// or generated by the local node(it is the leader)
func (consensus *ConsensusRaftImpl) saveSnap(snap raftpb.Snapshot) error {
	consensus.logger.InfoDynamic(func() string {
		return fmt.Sprintf("saveSnap %v", describeSnapshot(snap))
	})
	walSnap := walpb.Snapshot{
		Index:     snap.Metadata.Index,
		Term:      snap.Metadata.Term,
		ConfState: &snap.Metadata.ConfState,
	}

	if err := consensus.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := consensus.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := consensus.wal.ReleaseLockTo(snap.Metadata.Index); err != nil {
		return err
	}
	go consensus.PurgeFile(consensus.waldir)
	return nil
}

// PurgeFile purge wal file
func (consensus *ConsensusRaftImpl) PurgeFile(dirname string) {
	fileNames, err := fileutil.ReadDir(dirname)
	if err != nil {
		return
	}
	//当wal文件不超过2条时候，不处理
	if len(fileNames) <= 2 {
		return
	}

	walFileNames := make([]string, 0)
	for _, fileName := range fileNames {
		if strings.HasSuffix(fileName, ".wal") {
			walFileNames = append(walFileNames, fileName)
		}
	}

	for len(walFileNames) > 1 {
		f := filepath.Join(dirname, walFileNames[0])
		l, err := fileutil.TryLockFile(f, os.O_WRONLY, fileutil.PrivateFileMode)
		if err != nil {
			break
		}
		consensus.logger.Infof("[%x] PurgeFile Start: %s", consensus.Id, zap.String("fileName", l.Name()))
		if err = os.Remove(f); err != nil {
			return
		}
		if err = l.Close(); err != nil {
			consensus.logger.Infof("[%x] failed to unlock/close: %s", consensus.Id, zap.String("path", l.Name()), zap.Error(err))
			return
		}
		consensus.logger.Infof("[%x] PurgeFile Success: %s", consensus.Id, zap.String("fileName", l.Name()))
		walFileNames = walFileNames[1:]
	}
}

// serve is the serving goroutine, it processes tick events
// to pass to etcd-raft, readies received by etcd-raft,
// blocks proposed by core engine, and confChange events
// generated for etcd-raft
func (consensus *ConsensusRaftImpl) serve() {
	snapshot, err := consensus.raftStorage.Snapshot()
	if err != nil {
		consensus.logger.Fatalf("[%x] raftStorage Snapshot error", consensus.Id, err)
	}
	consensus.confState = snapshot.Metadata.ConfState
	consensus.snapshotIndex = snapshot.Metadata.Index
	consensus.appliedIndex = snapshot.Metadata.Index
	consensus.logger.InfoDynamic(func() string {
		return fmt.Sprintf("[%x] begin serve with snap: %v, appliedIndex: %v",
			consensus.Id, describeSnapshot(snapshot), consensus.appliedIndex)
	})

	tickTime := localconf.TechTradeChainConfig.ConsensusConfig.RaftConfig.Ticker
	if tickTime == 0 {
		tickTime = time.Nanosecond
	}
	ticker := time.NewTicker(tickTime * time.Second)

	// stop all before exit
	defer func() {
		ticker.Stop()
		consensus.wal.Close()
		consensus.node.Stop()
		consensus.msgbus.UnRegister(msgbus.ProposedBlock, consensus)
		consensus.msgbus.UnRegister(msgbus.RecvConsensusMsg, consensus)
		isStarted.Delete(consensus.raftKey)
		instances.Delete(consensus.raftKey)
	}()

	for {
		select {
		case <-ticker.C:
			consensus.node.Tick()
			consensus.logger.Debugf("[%x] status: %s", consensus.Id, consensus.node.Status())
		// process ready received from etcd-raft
		case ready := <-consensus.node.Ready():
			if exit := consensus.NodeReady(ready); exit {
				consensus.logger.Debugf("exit consensus when process ready message")
				return
			}
		// process block proposed by core engine,
		// only for leader node
		case block := <-consensus.proposedBlockC:
			consensus.ProposeBlock(block)
		// process confChange event generated for etcd-raft
		case cc := <-consensus.confChangeC:
			consensus.logger.DebugDynamic(func() string {
				return fmt.Sprintf("[%x] ProposeConfChange %v", consensus.Id, describeConfChange(cc))
			})
			if err := consensus.node.ProposeConfChange(context.TODO(), cc); err != nil {
				consensus.logger.Panicf("[%x] propose config change error: %v", consensus.Id, err)
			}
		}
	}
}

// NodeReady process ready msg from etcd-raft
func (consensus *ConsensusRaftImpl) NodeReady(ready etcdraft.Ready) (exit bool) {
	consensus.logger.DebugDynamic(func() string {
		return fmt.Sprintf("[%x] receive from raft ready, %v", consensus.Id, describeReady(ready))
	})
	if !etcdraft.IsEmptySnap(ready.Snapshot) && ready.Snapshot.Metadata.Index <= consensus.appliedIndex {
		consensus.logger.Fatalf("snapshot index: %v should > appliedIndex: %v",
			ready.Snapshot.Metadata.Index, consensus.appliedIndex)
	}
	//异步WalSave
	if consensus.asyncWalSave {
		consensus.walSaveC <- ready
	} else {
		consensus.processWalAndSnap(ready)
	}

	toExit, configChanged := consensus.publishEntries(consensus.entriesToApply(ready.CommittedEntries))
	height, err := consensus.ledgerCache.CurrentHeight()
	if err != nil {
		consensus.logger.Panic("get ledgerCache.CurrentHeight() error:%+v", err)
	}
	snapArgs := &SnapshotArgs{
		Index:       consensus.appliedIndex,
		BlockHeight: height,
		ConfChange:  configChanged,
	}
	if toExit {
		// wait for all the wal entries and snapshots to be processed before exit
		for len(consensus.walSaveC) != 0 {
			time.Sleep(500 * time.Millisecond)
		}
		close(consensus.closeC)
		consensus.wg.Wait()
		// save a snapshot before exit
		consensus.maybeTriggerSnapshot(snapArgs)
		consensus.logger.Infof("[%x] is deleted from consensus nodes", consensus.Id)
		return true
	}
	if consensus.asyncWalSave {
		consensus.walSaveC <- snapArgs
	} else {
		consensus.maybeTriggerSnapshot(snapArgs)
	}
	if ready.SoftState != nil {
		isLeader := consensus.isLeader
		leader := atomic.LoadUint64(&ready.SoftState.Lead)
		consensus.isLeader = leader == consensus.Id
		if value, ok := consensus.idToNodeId.Load(leader); ok {
			if proposer, ok := value.(string); ok {
				consensus.Proposer = proposer
			}
		}
		if !isLeader && consensus.isLeader {
			consensus.proposedIndex = 0
		}
	}
	consensus.sendProposeState(consensus.isLeader)
	return false
}

// ProposeBlock process proposed block from core engine
func (consensus *ConsensusRaftImpl) ProposeBlock(block *common.Block) {
	consensus.Lock()
	defer consensus.Unlock()
	// we have proposed a block in the same height
	if block.Header.BlockHeight <= consensus.proposedIndex {
		consensus.logger.Debugf("[%x] got proposed block in wrong height:%d",
			consensus.Id, block.Header.BlockHeight)
		return
	}
	// Add hash and signature to the block
	hash, sig, err := utils.SignBlock(consensus.chainConf.ChainConfig().Crypto.Hash, consensus.singer, block)
	if err != nil {
		consensus.logger.Errorf("[%x] sign block failed, %s", consensus.Id, err)
		return
	}
	block.Header.BlockHash = hash[:]
	block.Header.Signature = sig
	if block.AdditionalData == nil {
		block.AdditionalData = &common.AdditionalData{
			ExtraData: make(map[string][]byte),
		}
	}

	serializeMember, err := consensus.singer.GetMember()
	if err != nil {
		consensus.logger.Fatalf("[%x] get serialize member failed: %v", consensus.Id, err)
		return
	}

	signature := &common.EndorsementEntry{
		Signer:    serializeMember,
		Signature: sig,
	}

	additionalData := AdditionalData{
		Signature: mustMarshal(signature),
	}

	data, _ := json.Marshal(additionalData)
	block.AdditionalData.ExtraData[RAFTAddtionalDataKey] = data
	data = mustMarshal(block)
	consensus.logger.Debugf("[%x] propose block height：%+v", consensus.Id, block.Header.BlockHeight)
	// propose the block to the etcd-raft
	if err := consensus.node.Propose(context.TODO(), data); err != nil {
		consensus.logger.Panicf("[%x] propose error: %v", consensus.Id, err)
	}
	consensus.proposedIndex = block.Header.BlockHeight
}

// processWalAndSnap processes wal and snapshot from a ready received from etcd-raft
func (consensus *ConsensusRaftImpl) processWalAndSnap(ready etcdraft.Ready) {
	if !etcdraft.IsEmptyHardState(ready.HardState) || len(ready.Entries) != 0 {
		consensus.logger.Infof("[%x]Save wal: %v", consensus.Id, describeReady(ready))
		// save wal
		if err := consensus.wal.Save(ready.HardState, ready.Entries); err != nil {
			consensus.logger.Panicf("[%x] save wal: %v, error: %v", consensus.Id, describeReady(ready), err)
		}
	}
	if !etcdraft.IsEmptySnap(ready.Snapshot) {
		// save snapshot
		if err := consensus.saveSnap(ready.Snapshot); err != nil {
			consensus.logger.Panicf("[%x] save snap error: %v", consensus.Id, err)
		}
		// apply snapshot to raftStorage
		if err := consensus.raftStorage.ApplySnapshot(ready.Snapshot); err != nil {
			consensus.logger.Panicf("[%x] apply snapshot error: %v", consensus.Id, err)
		}
		// publish the snapshot
		consensus.publishSnapshot(ready.Snapshot)
	} else if !consensus.hasReportSnap && !consensus.isLeader {
		// to avoid that the current node was stopped when applying a snapshot,
		// and the MsgSnapStatus message was not sent to the leader in time,
		// we will have a try as soon as the node was started.
		for _, m := range ready.Messages {
			if m.Type == raftpb.MsgHeartbeatResp && consensus.snapshotIndex <= consensus.appliedIndex {
				msg := raftpb.Message{
					Type:       raftpb.MsgSnapStatus,
					From:       consensus.Id,
					To:         m.To,
					Index:      consensus.snapshotIndex,
					Reject:     false,
					RejectHint: consensus.lastHeight,
				}
				consensus.sendMessages([]raftpb.Message{msg})
				consensus.hasReportSnap = true
				break
			}
		}
	}
	// save the entries to raftStorage
	if err := consensus.raftStorage.Append(ready.Entries); err != nil {
		consensus.logger.Panicf("[%x] storage append entries error: %v", consensus.Id, err)
	}
	// we must send messages after save wal file
	consensus.sendMessages(ready.Messages)
	// notify that we have processed the ready to etcd-raft
	consensus.node.Advance()
}

// AsyncWalSave write wal asynchronously
func (consensus *ConsensusRaftImpl) AsyncWalSave() {
	for {
		select {
		case item := <-consensus.walSaveC:
			if ready, ok := item.(etcdraft.Ready); ok {
				consensus.processWalAndSnap(ready)
			} else if snapArgs, ok := item.(*SnapshotArgs); ok {
				consensus.maybeTriggerSnapshot(snapArgs)
			} else if confState, ok := item.(raftpb.ConfState); ok {
				consensus.confState = confState
			} else {
				consensus.logger.Panicf("[%x] AsyncWalSave got an invalid item: %v", item)
			}
		case <-consensus.closeC:
			consensus.wg.Done()
			return
		}
	}
}

// entriesToApply selects the log entries that need to apply
func (consensus *ConsensusRaftImpl) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}

	firstIdx := ents[0].Index
	// the log entries are discontinuous, with some intermediate entries missing
	if firstIdx > consensus.appliedIndex+1 {
		consensus.logger.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1",
			firstIdx, consensus.appliedIndex)
	}
	consensus.logger.Debugf("appliedIndex: %d, firstIndex: %d, entry num: %d", consensus.appliedIndex, firstIdx, len(ents))
	// remove duplicate log entries that have been already applied,
	// for example:
	//     appliedIndex == 5
	//     entries: 4,5,6,7,8
	//     then return entries [6,7,8]
	if consensus.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[consensus.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries processes log entries received from etcd-raft Ready.CommittedEntries
func (consensus *ConsensusRaftImpl) publishEntries(ents []raftpb.Entry) (exit bool, configChanged bool) {
	configChanged = false
	for i := range ents {
		consensus.logger.Debugf("publishEntries term: %d, index: %d, type: %v",
			ents[i].Term, ents[i].Index, ents[i].Type)
		switch ents[i].Type {
		// process a normal entry
		case raftpb.EntryNormal:
			// it is an empty entry with no blocks
			if len(ents[i].Data) == 0 {
				break
			}
			// unmarshal the techTradeChain block from the log entry
			block := new(common.Block)
			mustUnmarshal(ents[i].Data, block)
			consensus.logger.Debugf("publishEntries term: %d, index: %d, block(%d-%x)",
				ents[i].Term, ents[i].Index, block.Header.BlockHeight, block.Header.BlockHash)
			// verify and commit the block by calling the core engine
			consensus.commitBlock(block)
			if utils.IsConfBlock(block) {
				// push the configuration changing to the etcd-raft
				consensus.processConfigChange()
			}
		// process a confChange log entry
		case raftpb.EntryConfChange:
			configChanged = true
			var cc raftpb.ConfChange
			// unmarshal the etcd-raft confChange log entry
			if err := cc.Unmarshal(ents[i].Data); err != nil {
				consensus.logger.Panicf("[%x] unmarshal config change error: %v", consensus.Id, err)
			}
			// apply the confChange log entry to the etcd-raft
			confState := *consensus.node.ApplyConfChange(cc)
			// update the confState
			if consensus.asyncWalSave {
				consensus.walSaveC <- confState
			} else {
				consensus.confState = confState
			}
			// update the consensus nodes list
			var idToNodes map[uint64]string
			consensus.peers, idToNodes = consensus.getPeersFromChainConf()
			for id, node := range idToNodes {
				consensus.idToNodeId.Store(id, node)
			}
			// if the local node is about to be removed, the consensus module will exit right now
			switch cc.Type {
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == consensus.Id {
					consensus.appliedIndex = ents[i].Index
					return true, configChanged
				}
			}
		}
		consensus.appliedIndex = ents[i].Index
	}
	return false, configChanged
}

// publishSnapshot processes a snapshot received from a etcd-raft Ready
func (consensus *ConsensusRaftImpl) publishSnapshot(snapshot raftpb.Snapshot) {
	// whether the snapshot is successfully applied or not,
	// we should notify the result to the leader
	result := etcdraft.SnapshotFinish
	var current uint64
	defer func() {
		if !consensus.isLeader {
			msg := raftpb.Message{
				Type:       raftpb.MsgSnapStatus,
				From:       consensus.Id,
				To:         computeRaftIdFromNodeId(consensus.Proposer),
				Index:      snapshot.Metadata.Index,
				Reject:     result == etcdraft.SnapshotFailure,
				RejectHint: current,
			}
			consensus.sendMessages([]raftpb.Message{msg})
			consensus.hasReportSnap = true
		}
	}()

	if etcdraft.IsEmptySnap(snapshot) {
		return
	}

	consensus.logger.Infof("publishSnapshot metadata: %v", snapshot.Metadata)
	consensus.confState = snapshot.Metadata.ConfState
	consensus.snapshotIndex = snapshot.Metadata.Index
	if snapshot.Metadata.Index > consensus.appliedIndex {
		consensus.appliedIndex = snapshot.Metadata.Index
	}

	// get the block height from the snapshot
	snapshotData := &SnapshotHeight{}
	err := json.Unmarshal(snapshot.Data, snapshotData)
	if err != nil {
		result = etcdraft.SnapshotFailure
		consensus.logger.Panicf("unmarshal snapshot error")
	}

	// Loop until catch up to snapshotData.Height from Sync module
	for {
		current, _ = consensus.ledgerCache.CurrentHeight()
		consensus.logger.Debugf("publishSnapshot current height: %d, snapshot height: %d", current, snapshotData.Height)
		if current >= snapshotData.Height {
			break
		}
		time.Sleep(500 * time.Microsecond)
	}
}

// getSnapshot makes a snapshot with only a block height
func (consensus *ConsensusRaftImpl) getSnapshot(height uint64) ([]byte, error) {
	data, err := json.Marshal(SnapshotHeight{
		Height: height,
	})
	consensus.logger.Infof("getSnapshot data: %s", data)
	return data, err
}

// maybeTriggerSnapshot makes a snapshot and processes it if necessary
func (consensus *ConsensusRaftImpl) maybeTriggerSnapshot(snapArgs *SnapshotArgs) {
	// configuration changing must trigger a snapshot
	if snapArgs.Index <= consensus.snapshotIndex ||
		snapArgs.Index-consensus.snapshotIndex <= consensus.snapCount && !snapArgs.ConfChange {
		return
	}

	// get a snapshot with the block height
	data, err := consensus.getSnapshot(snapArgs.BlockHeight)
	if err != nil {
		consensus.logger.Fatalf("get snapshot error: %v", err)
	}

	// construct a etcd-raft Snapshot, if failed, the progress will exit
	snap, err := consensus.raftStorage.CreateSnapshot(snapArgs.Index, &consensus.confState, data)
	if err != nil {
		consensus.logger.Fatalf("create snapshot error: %v", err)
	}

	// save the snapshot in the local file system
	if err := consensus.saveSnap(snap); err != nil {
		consensus.logger.Fatalf("save snapshot error: %v", err)
	}

	consensus.logger.Infof("trigger snapshot Index: %v, data: %v, snapshotIndex: %v",
		snapArgs.Index, string(data), consensus.snapshotIndex)
	consensus.snapshotIndex = snap.Metadata.Index

	// not need to compact for the first few log entries
	if consensus.snapshotIndex < snapshotCatchUpEntriesN+1 {
		return
	}

	// check the index to avoid panic in etcd-raft when compacting logs
	compactIndex := consensus.snapshotIndex - snapshotCatchUpEntriesN
	first, _ := consensus.raftStorage.FirstIndex()
	if compactIndex <= first {
		return
	}
	// truncate log entries in raftStorage, only retain 5 log entries
	// before the last snapshot index.
	// the progress will exit when compact failed
	if err := consensus.raftStorage.Compact(compactIndex); err != nil {
		last, _ := consensus.raftStorage.LastIndex()
		consensus.logger.Fatalf("compact snapshot error: %v, compact "+
			"index: %d, first: %d, last: %d", err, compactIndex, first, last)
	}

	consensus.logger.Infof("compact entries compactIndex:%d", compactIndex)
}

// sendMessages processes a group of etcd-raft Messages,
// marshals and packages them to NetMsg in techTradeChain,
// and sends them to the destination nodes one by one
// via pushing them to the message bus
func (consensus *ConsensusRaftImpl) sendMessages(msgs []raftpb.Message) {
	for _, m := range msgs {
		// there is no destination
		if m.To == 0 {
			consensus.logger.Errorf("send message to 0")
			continue
		}

		consensus.logger.DebugDynamic(func() string {
			return fmt.Sprintf("[%x] send message %v", consensus.Id, describeMessage(m))
		})

		// get the techTradeChain nodeId by the etcd-raft nodeId
		value, ok := consensus.idToNodeId.Load(m.To)
		if !ok {
			consensus.logger.Errorf("send message to %v without net connection", m.To)
		} else {
			netId, ok := value.(string)
			if !ok {
				consensus.logger.Errorf("wrong type in idToNodeId")
				continue
			}
			// marshal the etcd-raft Message
			data, err := m.Marshal()
			if err != nil {
				consensus.logger.Errorf("marshal message error: %v", err)
				continue
			}
			// package the marshaled data to a techTradeChain NetMsg
			netMsg := &netpb.NetMsg{
				Payload: data,
				Type:    netpb.NetMsg_CONSENSUS_MSG,
				To:      netId,
			}
			// push the netMsg to the message bus to send
			consensus.msgbus.Publish(msgbus.SendConsensusMsg, netMsg)
		}
	}
}

// loadSnapshot loads the snapshot from the local file system
func (consensus *ConsensusRaftImpl) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := consensus.snapshotter.Load()
	// if load the snapshot from the local file system failed,
	// there are something wrong in the files, the progress will exit
	if err != nil && err != snap.ErrNoSnapshot {
		consensus.logger.Fatalf("load snapshot error: %v", err)
	}
	if snapshot == nil {
		consensus.logger.Infof("loadSnapshot snapshot is nil")
	} else {
		consensus.logger.Infof("loadSnapshot snapshot metadata index: %v", snapshot.Metadata.Index)
	}
	return snapshot
}

// replayWAL was used to replay the history entries that has been recorded
// in the wal file, after the current node was restarted
func (consensus *ConsensusRaftImpl) replayWAL() *wal.WAL {
	// if there was no wal file in the current node,
	// the local node has been started by the first time,
	// so we must prepare the directory and the wal file
	// in the system for it
	if !wal.Exist(consensus.waldir) {
		if err := os.Mkdir(consensus.waldir, 0750); err != nil {
			consensus.logger.Fatalf("cannot create wal dir: %v", err)
		}

		w, err := wal.Create(consensus.logger.(*logger.CMLogger).Logger().Desugar(), consensus.waldir, nil)
		if err != nil {
			consensus.logger.Fatalf("create wal error: %v", err)
		}
		w.Close()
	}

	// load the snapshot information from the snapshot file
	snapshot := consensus.loadSnapshot()

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	// open the wal file, and get the file handler,
	// stop the progress if failed.
	// To fix the "max entry size limit exceeded" error
	// returned by etcd-raft, set the limit to zero.
	w, err := wal.Open(consensus.logger.(*logger.CMLogger).Logger().Desugar(), consensus.waldir, walsnap, 0)
	if err != nil {
		consensus.logger.Fatalf("open wal error: %v", err)
	}

	// get the all entries from the wal file
	_, state, ents, err := w.ReadAll()
	if err != nil {
		consensus.logger.Fatalf("read wal error: %v", err)
	}

	// initialize the raftStorage object, and apply the snapshot
	// to the raftStorage if there is one
	consensus.raftStorage = etcdraft.NewMemoryStorage()
	if snapshot != nil {
		if err := consensus.raftStorage.ApplySnapshot(*snapshot); err != nil {
			consensus.logger.Panicf("[%x] apply snapshot error: %v", consensus.Id, err)
		}
	}
	// set the hard state(term, vote, commit) to the raftStorage
	if err := consensus.raftStorage.SetHardState(state); err != nil {
		consensus.logger.Panicf("[%x] SetHardState error: %v", consensus.Id, err)
	}
	// append the log entries to the raft storage
	if err := consensus.raftStorage.Append(ents); err != nil {
		consensus.logger.Panicf("[%x] storage append error: %v", consensus.Id, err)
	}
	consensus.logger.Infof("replayWAL walsnap index: %v, len(ents): %v", walsnap.Index, len(ents))
	return w
}

// commitBlock verifies and commits a specified block by calling the core engine
func (consensus *ConsensusRaftImpl) commitBlock(block *common.Block) {
	// verify the block repeatedly util success,
	// so if a block was verified failed over and over again,
	// the current node will fall behind in consensus,
	// as it will be unable to participate in the subsequent consensus process,
	// till the sync module commit the block success
	for {
		err := consensus.blockVerifier.VerifyBlock(block, protocol.CONSENSUS_VERIFY)
		consensus.logger.Debugf("verify block: %d-%x error: %v", block.Header.BlockHeight, block.Header.BlockHash, err)
		if err == nil {
			break
		}

		// the block was committed by the sync module
		if err == commonErrors.ErrBlockHadBeenCommited {
			return
		}

		// the core engine has already been verifying a block with the same height and hash
		if err == commonErrors.ErrConcurrentVerify {
			consensus.logger.Warnf("verify block: %d-%x fail: %s", block.Header.BlockHeight, block.Header.BlockHash, err)
		} else {
			consensus.logger.Errorf("verify block: %d-%x fail: %s", block.Header.BlockHeight, block.Header.BlockHash, err)
		}
		time.Sleep(time.Millisecond * 10)
	}

	// call core engine to commit the block to the block chain
	err := consensus.blockCommitter.AddBlock(block)
	consensus.lastHeight = block.Header.BlockHeight
	consensus.logger.Debugf("commit block: %d-%x error: %v", block.Header.BlockHeight, block.Header.BlockHash, err)

	// the block was committed failed, there was something wrong with the block chain
	if err != nil && err != commonErrors.ErrBlockHadBeenCommited {
		consensus.logger.Fatalf("commit block: %d-%x fail: %s", block.Header.BlockHeight, block.Header.BlockHash, err)
	}
}

// sendProposeState syncs the information to the core engine,
// tells that weather the current node is the leader in the
// consensus validators
func (consensus *ConsensusRaftImpl) sendProposeState(isProposer bool) {
	consensus.logger.Infof("sendProposeState isProposer: %v", isProposer)
	consensus.msgbus.PublishSafe(msgbus.ProposeState, isProposer)
}

// Verify implements interface of struct Verifier,
// This interface is used to verify the validity of parameters,
// it executes before consensus.
func (consensus *ConsensusRaftImpl) Verify(
	consensusType consensuspb.ConsensusType,
	chainConfig *config.ChainConfig) error {
	return nil
}

// getPeersFromChainConf gets the current consensus nodes' Ids, and use them to calculate
// the nodeIds for the etcd-raft, and returns the mapping between the two
func (consensus *ConsensusRaftImpl) getPeersFromChainConf() ([]uint64, map[uint64]string) {
	var (
		peers      []uint64
		idToNodeId = make(map[uint64]string)
		builder    strings.Builder
	)

	fmt.Fprintf(&builder, "[")
	for _, org := range consensus.chainConf.ChainConfig().Consensus.Nodes {
		for _, nodeId := range org.NodeId {
			id := computeRaftIdFromNodeId(nodeId)
			idToNodeId[id] = nodeId
			peers = append(peers, id)
			fmt.Fprintf(&builder, "%s: %x, ", nodeId, id)
		}
	}
	fmt.Fprintf(&builder, "]")

	consensus.logger.InfoDynamic(func() string {
		return fmt.Sprintf("[%x] getPeersFromChainConf peers: %v", consensus.Id, builder.String())
	})
	sort.Slice(peers, func(i, j int) bool {
		return peers[i] < peers[j]
	})
	return peers, idToNodeId
}

// processConfigChange generates the etcd ConfChange entries
// for the configuration-change blocks, and inserts them to
// the main process, if there are consensus nodes are added or removed
func (consensus *ConsensusRaftImpl) processConfigChange() bool {
	peers, idToNodes := consensus.getPeersFromChainConf()
	removed, added := computeUpdatedNodes(consensus.peers, peers)
	consensus.logger.Debugf("[%x] processConfigChange removed: %v, added: %v", consensus.Id, removed, added)

	if consensus.isLeader {
		// generates the RemoveNode entries, and inserts them
		// to the main process via the channel confChangeC
		for _, node := range removed {
			cc := raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: node,
			}
			consensus.confChangeC <- cc
		}
		// generates the AddNode entries, and inserts them
		// to the main process via the channel confChangeC
		for _, node := range added {
			cc := raftpb.ConfChange{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: node,
			}
			consensus.confChangeC <- cc
		}
		consensus.peers = peers
		for id, node := range idToNodes {
			consensus.idToNodeId.Store(id, node)
		}
	}
	return len(removed) != 0 || len(added) != 0
}

// VerifyBlockSignatures verifies whether the signatures in block
// is qulified with the consensus algorithm. It should return nil
// error when verify successfully, and return corresponding error
// when failed.
func VerifyBlockSignatures(block *common.Block) error {
	if block == nil || block.Header == nil ||
		block.AdditionalData == nil || block.AdditionalData.ExtraData == nil {
		return fmt.Errorf("invalid block")
	}
	byt, ok := block.AdditionalData.ExtraData[RAFTAddtionalDataKey]
	if !ok {
		return fmt.Errorf("block.AdditionalData.ExtraData[RAFTAddtionalDataKey] not exist")
	}

	additionalData := &AdditionalData{}
	err := json.Unmarshal(byt, additionalData)
	if err != nil {
		return fmt.Errorf("block.AdditionalData.ExtraData[RAFTAddtionalDataKey] unmarshal failed:%+v", err)
	}

	endorsement := new(common.EndorsementEntry)
	mustUnmarshal(additionalData.Signature, endorsement)

	if !bytes.Equal(block.Header.Signature, endorsement.Signature) {
		return fmt.Errorf("block.AdditionalData.ExtraData[RAFTAddtionalDataKey] not exist")
	}
	return nil
}

// computeRaftIdFromNodeId calculates the nodeId in etcd-raft (uint64) by
// the nodeId in the techTradeChain (string)
func computeRaftIdFromNodeId(nodeId string) uint64 {
	return uint64(binary.BigEndian.Uint64([]byte(nodeId[len(nodeId)-8:])))
}

func computeUpdatedNodes(oldSet, newSet []uint64) (removed []uint64, added []uint64) {
	removedSet, addedSet := funk.Difference(oldSet, newSet)

	return removedSet.([]uint64), addedSet.([]uint64)
}
