/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package utils

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
	consensusPb "techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/utils/v2/cache"
	"github.com/gogo/protobuf/proto"
)

const (
	// NativePrefix black list profix
	NativePrefix = "vm-native"
	// Violation black list message
	Violation = "该交易违规，内容已屏蔽。"
)

// CalcBlockHash calculate block hash
func CalcBlockHash(hashType string, b *commonPb.Block) ([]byte, error) {
	if b == nil {
		return nil, fmt.Errorf("calc hash block == nil")
	}
	blockBytes, err := calcUnsignedBlockBytes(b)
	if err != nil {
		return nil, err
	}
	return hash.GetByStrType(hashType, blockBytes)
}

// SignBlock sign the block (in fact, here we sign block hash...) with signing member
// return hash bytes and signature bytes
func SignBlock(hashType string, singer protocol.SigningMember, b *commonPb.Block) ([]byte, []byte, error) {
	if singer == nil {
		return nil, nil, fmt.Errorf("sign block signer == nil")
	}
	blockHash, err := CalcBlockHash(hashType, b)
	if err != nil {
		return []byte{}, nil, err
	}
	sig, err := singer.Sign(hashType, blockHash)
	if err != nil {
		return nil, nil, err
	}
	return blockHash, sig, nil
}

// FormatBlock format block into string
func FormatBlock(b *commonPb.Block) string {
	serializedBlock := bytes.Buffer{}
	serializedBlock.WriteString("-------------------block begins-----------------\n")
	serializedBlock.WriteString(fmt.Sprintf("ChainId:\t%s\n", b.Header.ChainId))
	serializedBlock.WriteString(fmt.Sprintf("BlockHeight:\t%d\n", b.Header.BlockHeight))
	serializedBlock.WriteString(fmt.Sprintf("PreBlockHash:\t%x\n", b.Header.PreBlockHash))
	serializedBlock.WriteString(fmt.Sprintf("PreConfHeight:\t%d\n", b.Header.PreConfHeight))
	serializedBlock.WriteString(fmt.Sprintf("BlockVersion:\t%x\n", b.Header.BlockVersion))
	serializedBlock.WriteString(fmt.Sprintf("DagHash:\t%x\n", b.Header.DagHash))
	serializedBlock.WriteString(fmt.Sprintf("RwSetRoot:\t%x\n", b.Header.RwSetRoot))
	serializedBlock.WriteString(fmt.Sprintf("TxRoot:\t%x\n", b.Header.TxRoot))
	serializedBlock.WriteString(fmt.Sprintf("BlockTimestamp:\t%d\n", b.Header.BlockTimestamp))
	serializedBlock.WriteString(fmt.Sprintf("Proposer:\t%x\n", b.Header.Proposer))
	serializedBlock.WriteString(fmt.Sprintf("ConsensusArgs:\t%x\n", b.Header.ConsensusArgs))
	serializedBlock.WriteString(fmt.Sprintf("TxCount:\t%d\n", b.Header.TxCount))
	serializedBlock.WriteString("------------block signed part ends-------------\n")
	serializedBlock.WriteString(fmt.Sprintf("BlockHash:\t%x\n", b.Header.BlockHash))
	serializedBlock.WriteString(fmt.Sprintf("Signature:\t%x\n", b.Header.Signature))
	serializedBlock.WriteString("------------block unsigned part ends-----------\n")
	return serializedBlock.String()
}

// calcUnsignedBlockBytes calculate unsigned block bytes
// since dag & txs are already included in block header, we can safely set this two field to nil
func calcUnsignedBlockBytes(b *commonPb.Block) ([]byte, error) {
	//block := &commonPb.Block{
	//	Header: &commonPb.BlockHeader{
	//		ChainId:        b.Header.ChainId,
	//		BlockHeight:    b.Header.BlockHeight,
	//		PreBlockHash:   b.Header.PreBlockHash,
	//		BlockHash:      nil,
	//		PreConfHeight:  b.Header.PreConfHeight,
	//		BlockVersion:   b.Header.BlockVersion,
	//		DagHash:        b.Header.DagHash,
	//		RwSetRoot:      b.Header.RwSetRoot,
	//		TxRoot:         b.Header.TxRoot,
	//		BlockTimestamp: b.Header.BlockTimestamp,
	//		Proposer:       b.Header.Proposer,
	//		ConsensusArgs:  b.Header.ConsensusArgs,
	//		TxCount:        b.Header.TxCount,
	//		Signature:      nil,
	//	},
	//	Dag: nil,
	//	Txs: nil,
	//}
	//BlockHash就是HeaderHash，所以这里只需要把Header的Signature和BlockHash字段去掉，再序列化计算Hash即可。
	header := *b.Header
	header.Signature = nil
	header.BlockHash = nil
	blockBytes, err := proto.Marshal(&header)
	if err != nil {
		return nil, err
	}
	return blockBytes, nil
}

// BlockFingerPrint alias for string
type BlockFingerPrint string

// CalcBlockFingerPrintWithoutTx 排除掉Tx的因素，计算Block的指纹，这样计算出来不管Block如何包含Tx，其指纹不变
func CalcBlockFingerPrintWithoutTx(block *commonPb.Block) BlockFingerPrint {
	if block == nil {
		return ""
	}
	chainId := block.Header.ChainId
	blockHeight := block.Header.BlockHeight
	blockTimestamp := block.Header.BlockTimestamp
	var blockProposer []byte
	if block.Header.Proposer != nil {
		blockProposer, _ = block.Header.Proposer.Marshal()
	}
	preBlockHash := block.Header.PreBlockHash

	return CalcFingerPrint(chainId, blockHeight, blockTimestamp, blockProposer, preBlockHash,
		nil, nil, nil)
}

// CalcBlockFingerPrint since the block has not yet formed,
//snapshot uses fingerprint as the possible unique value of the block
func CalcBlockFingerPrint(block *commonPb.Block) BlockFingerPrint {
	if block == nil {
		return ""
	}
	chainId := block.Header.ChainId
	blockHeight := block.Header.BlockHeight
	blockTimestamp := block.Header.BlockTimestamp
	var blockProposer []byte
	if block.Header.Proposer != nil {
		blockProposer, _ = block.Header.Proposer.Marshal()
	}
	preBlockHash := block.Header.PreBlockHash

	return CalcFingerPrint(chainId, blockHeight, blockTimestamp, blockProposer, preBlockHash,
		block.Header.TxRoot, block.Header.DagHash, block.Header.RwSetRoot)
}

// CalcFingerPrint calculate finger print
func CalcFingerPrint(chainId string, height uint64, timestamp int64,
	proposer, preHash, txRoot, dagHash, rwSetRoot []byte) BlockFingerPrint {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s-%v-%v-%v-%v-%v-%v-%v", chainId, height, timestamp,
		proposer, preHash, txRoot, dagHash, rwSetRoot)))
	return BlockFingerPrint(fmt.Sprintf("%x", h.Sum(nil)))
}

// CalcPartialBlockHash calculate partial block bytes
// hash contains Header without BlockHash, ConsensusArgs, Signature
func CalcPartialBlockHash(hashType string, b *commonPb.Block) ([]byte, error) {
	if b == nil {
		return nil, fmt.Errorf("calc partial hash block == nil")
	}
	block := &commonPb.Block{
		Header: &commonPb.BlockHeader{
			ChainId:        b.Header.ChainId,
			BlockHeight:    b.Header.BlockHeight,
			PreBlockHash:   b.Header.PreBlockHash,
			BlockHash:      nil,
			PreConfHeight:  b.Header.PreConfHeight,
			BlockVersion:   b.Header.BlockVersion,
			DagHash:        b.Header.DagHash,
			RwSetRoot:      b.Header.RwSetRoot,
			TxRoot:         b.Header.TxRoot,
			BlockTimestamp: b.Header.BlockTimestamp,
			Proposer:       b.Header.Proposer,
			ConsensusArgs:  nil,
			TxCount:        b.Header.TxCount,
			Signature:      nil,
		},
		Dag: nil,
		Txs: nil,
	}

	blockBytes, err := proto.Marshal(block)
	if err != nil {
		return nil, err
	}
	return hash.GetByStrType(hashType, blockBytes)
}

// IsConfBlock is it a configuration block
func IsConfBlock(block *commonPb.Block) bool {
	if block == nil || len(block.Txs) == 0 {
		return false
	}
	tx := block.Txs[0]
	return IsValidConfigTx(tx)
}

// GetConsensusArgsFromBlock get args from block
func GetConsensusArgsFromBlock(block *commonPb.Block) (*consensusPb.BlockHeaderConsensusArgs, error) {
	if block == nil {
		return nil, fmt.Errorf("block is nil")
	}
	args := new(consensusPb.BlockHeaderConsensusArgs)
	if block.Header.ConsensusArgs == nil {
		return nil, fmt.Errorf("ConsensusArgs is nil")
	}
	err := proto.Unmarshal(block.Header.ConsensusArgs, args)
	if err != nil {
		return nil, fmt.Errorf("Unmarshal err:%s", err)
	}
	return args, nil
}

// IsEmptyBlock is it a empty block
func IsEmptyBlock(block *commonPb.Block) error {
	if block == nil || block.Header == nil || block.Header.BlockHash == nil ||
		block.Header.ChainId == "" || block.Header.PreBlockHash == nil || block.Header.Signature == nil ||
		block.Header.Proposer == nil || block.Dag == nil {
		return fmt.Errorf("invalid block, nil field, yield verify")
	}
	return nil
}

// CanProposeEmptyBlock can empty blocks be packed
func CanProposeEmptyBlock(consensusType consensusPb.ConsensusType) bool {
	return consensusPb.ConsensusType_MAXBFT == consensusType || consensusPb.ConsensusType_POW == consensusType
}

// VerifyBlockSig verify block proposer and signature
func VerifyBlockSig(hashType string, b *commonPb.Block, ac protocol.AccessControlProvider) (bool, error) {
	hashedBlock, err := CalcBlockHash(hashType, b)
	if err != nil {
		return false, fmt.Errorf("fail to hash block: %v", err)
	}
	var member = b.Header.Proposer
	endorsements := []*commonPb.EndorsementEntry{{
		Signer:    member,
		Signature: b.Header.Signature,
	}}
	principal, err := ac.CreatePrincipal(protocol.ResourceNameConsensusNode, endorsements, hashedBlock)
	if err != nil {
		return false, fmt.Errorf("fail to construct authentication principal: %v", err)
	}
	ok, err := ac.VerifyMsgPrincipal(principal, b.Header.BlockVersion)
	if err != nil {
		return false, fmt.Errorf("authentication fail: %v", err)
	}
	if !ok {
		return false, fmt.Errorf("authentication fail")
	}
	return true, nil
}

// IsContractMgmtBlock check is contract management block
func IsContractMgmtBlock(b *commonPb.Block) bool {
	if len(b.Txs) == 0 {
		return false
	}
	return IsContractMgmtTx(b.Txs[0])
}

// FilterBlockTxs filter transactions with given sender org id
func FilterBlockTxs(reqSenderOrgId string, block *commonPb.Block) *commonPb.Block {

	txs := block.GetTxs()
	results := make([]*commonPb.Transaction, 0, len(txs))

	newBlock := &commonPb.Block{
		Header:         block.Header,
		Dag:            block.Dag,
		AdditionalData: block.AdditionalData,
	}
	for i, tx := range txs {
		if block.Header.BlockHeight != 0 && tx.Sender.Signer.OrgId == reqSenderOrgId {
			results = append(results, txs[i])
		}
	}
	newBlock.Txs = results
	return newBlock
}

// FilterBlockBlacklistTxRWSet filter txRWSet in blacklist tx id
func FilterBlockBlacklistTxRWSet(txRWSet []*commonPb.TxRWSet, chainId string) (txRWSetNew []*commonPb.TxRWSet) {
	if len(txRWSet) == 0 {
		return txRWSet
	}
	bl := cache.NewCacheList(NativePrefix + chainId)
	// add priority judgment, because bl is empty in most cases
	if bl.Empty() {
		return txRWSet
	}
	txRWSetNew = make([]*commonPb.TxRWSet, 0, len(txRWSet))
	for i := range txRWSet {
		if bl.Exists(txRWSet[i].TxId) {
			txRWSetNew = append(txRWSetNew, &commonPb.TxRWSet{TxId: txRWSet[i].TxId})
		} else {
			txRWSetNew = append(txRWSetNew, txRWSet[i])
		}
	}
	return txRWSetNew
}

// FilterBlockBlacklistEvents filter events in blacklist tx id
func FilterBlockBlacklistEvents(events []*commonPb.ContractEvent, chainId string) []*commonPb.ContractEvent {
	if len(events) == 0 {
		return events
	}
	bl := cache.NewCacheList(NativePrefix + chainId)
	// add priority judgment, because bl is empty in most cases
	if bl.Empty() {
		return events
	}
	eventsNew := make([]*commonPb.ContractEvent, 0, len(events))
	for i := range events {
		if bl.Exists(events[i].TxId) {
			eventsNew = append(eventsNew, &commonPb.ContractEvent{TxId: events[i].TxId, ContractName: Violation})
		} else {
			eventsNew = append(eventsNew, events[i])
		}
	}
	return eventsNew
}

// FilterBlockBlacklistTxs filter transactions with blacklist
func FilterBlockBlacklistTxs(block *commonPb.Block) *commonPb.Block {
	newBlock := &commonPb.Block{
		Header:         block.Header,
		Dag:            block.Dag,
		AdditionalData: block.AdditionalData,
	}
	newBlock.Txs = FilterBlacklistTxs(block.GetTxs())
	return newBlock
}

// FilterBlacklistTxs filter transactions with blacklist
func FilterBlacklistTxs(txs []*commonPb.Transaction) []*commonPb.Transaction {
	if len(txs) == 0 {
		return txs
	}
	chainId := txs[0].Payload.ChainId
	bl := cache.NewCacheList(NativePrefix + chainId)
	// add priority judgment, because bl is empty in most cases
	if bl.Empty() {
		return txs
	}
	results := make([]*commonPb.Transaction, 0, len(txs))
	for i := range txs {
		if !bl.Exists(txs[i].Payload.TxId) {
			results = append(results, txs[i])
			continue
		}

		contractResult := txs[i].Result.ContractResult
		if contractResult == nil {
			contractResult = &commonPb.ContractResult{}
		}
		tx := &commonPb.Transaction{
			Payload: &commonPb.Payload{
				ChainId:        txs[i].Payload.ChainId,
				TxType:         txs[i].Payload.TxType,
				TxId:           txs[i].Payload.TxId,
				Timestamp:      txs[i].Payload.Timestamp,
				ExpirationTime: txs[i].Payload.ExpirationTime,
				ContractName:   Violation,
				Method:         Violation,
				Parameters:     nil,
				Sequence:       txs[i].Payload.Sequence,
				Limit:          txs[i].Payload.Limit,
			},
			Sender:    txs[i].Sender,
			Endorsers: txs[i].Endorsers,
			Result: &commonPb.Result{
				Code: txs[i].Result.Code,
				ContractResult: &commonPb.ContractResult{
					Code:          contractResult.Code,
					Result:        nil,
					Message:       Violation,
					GasUsed:       contractResult.GasUsed,
					ContractEvent: nil,
				},
				RwSetHash: txs[i].Result.RwSetHash,
				Message:   Violation,
			},
			Payer: txs[i].Payer,
		}
		results = append(results, tx)
	}
	return results
}

// HasDPosTxWritesInHeader check if header has DPoS tx writes
func HasDPosTxWritesInHeader(block *commonPb.Block, chainConf protocol.ChainConf) bool {
	consensusType := chainConf.ChainConfig().Consensus.Type
	return consensusType == consensusPb.ConsensusType_DPOS && len(block.Header.ConsensusArgs) > 0
}
