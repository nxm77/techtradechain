/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package techtradechain_sdk_go

import (
	"errors"
	"fmt"
	"strings"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	"techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/sdk-go/v2/utils"
	_ "github.com/go-sql-driver/mysql"
)

// CreateArchiveBlockPayload create `archive block` payload
func (cc *ChainClient) CreateArchiveBlockPayload(targetBlockHeight uint64) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [Archive] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   syscontract.ArchiveBlock_BLOCK_HEIGHT.String(),
			Value: utils.U64ToBytes(targetBlockHeight),
		},
	}

	payload := cc.CreatePayload("", common.TxType_ARCHIVE, syscontract.SystemContract_ARCHIVE_MANAGE.String(),
		syscontract.ArchiveFunction_ARCHIVE_BLOCK.String(), pairs, defaultSeq, nil)

	return payload, nil
}

// CreateRestoreBlockPayload create `restore block` payload
func (cc *ChainClient) CreateRestoreBlockPayload(fullBlock []byte) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [restore] to be signed payload")

	pairs := []*common.KeyValuePair{
		{
			Key:   syscontract.RestoreBlock_FULL_BLOCK.String(),
			Value: fullBlock,
		},
	}

	payload := cc.CreatePayload("", common.TxType_ARCHIVE, syscontract.SystemContract_ARCHIVE_MANAGE.String(),
		syscontract.ArchiveFunction_RESTORE_BLOCK.String(), pairs, defaultSeq, nil)

	return payload, nil
}

// CreateRestoreBlocksPayload create `restore blocks` payload
func (cc *ChainClient) CreateRestoreBlocksPayload(fullBlocks [][]byte) (*common.Payload, error) {
	cc.logger.Debugf("[SDK] create [restore blocks] to be signed payload")
	pairs := make([]*common.KeyValuePair, 0, len(fullBlocks))
	for i := 0; i < len(fullBlocks); i++ {
		pairs = append(pairs, &common.KeyValuePair{
			Key:   syscontract.RestoreBlock_FULL_BLOCK.String(),
			Value: fullBlocks[i],
		})
	}
	payload := cc.CreatePayload("", common.TxType_ARCHIVE, syscontract.SystemContract_ARCHIVE_MANAGE.String(),
		syscontract.ArchiveFunction_RESTORE_BLOCK.String(), pairs, defaultSeq, nil)

	return payload, nil
}

// SendArchiveBlockRequest send `archive block` request to node grpc server
func (cc *ChainClient) SendArchiveBlockRequest(payload *common.Payload, timeout int64) (*common.TxResponse, error) {
	return cc.proposalRequest(payload, nil, nil, timeout, false)
}

// SendRestoreBlockRequest send `restore block` request to node grpc server
func (cc *ChainClient) SendRestoreBlockRequest(payload *common.Payload, timeout int64) (*common.TxResponse, error) {
	return cc.proposalRequest(payload, nil, nil, timeout, false)
}

// GetArchivedBlockByHeight get archived block by block height, returns *common.BlockInfo
func (cc *ChainClient) GetArchivedBlockByHeight(blockHeight uint64, withRWSet bool) (*common.BlockInfo, error) {
	return cc.GetArchiveService().GetBlockByHeight(blockHeight, withRWSet)
}

// GetArchivedBlockByTxId get archived block by tx id, returns *common.BlockInfo
func (cc *ChainClient) GetArchivedBlockByTxId(txId string, withRWSet bool) (*common.BlockInfo, error) {
	return cc.GetArchiveService().GetBlockByTxId(txId, withRWSet)
}

// GetArchivedBlockByHash get archived block by block hash, returns *common.BlockInfo
func (cc *ChainClient) GetArchivedBlockByHash(blockHash string, withRWSet bool) (*common.BlockInfo, error) {
	return cc.GetArchiveService().GetBlockByHash(blockHash, withRWSet)
}

// GetArchivedTxByTxId get archived tx by tx id, returns *common.TransactionInfo
func (cc *ChainClient) GetArchivedTxByTxId(txId string) (*common.TransactionInfo, error) {
	return cc.GetArchiveService().GetTxByTxId(txId)
}

// ArchiveBlocks 归档指定区块高度范围的区块
// @param beginHeight
// @param endHeight
// @param mode
// @param heightNotice
// @return error
func (cc *ChainClient) ArchiveBlocks(archiveHeight uint64, mode string,
	heightNotice func(ProcessMessage) error) error {
	cc.logger.Debugf("do archive blocks to height[%d], check status and calculate begin & end height",
		archiveHeight)
	//1.检查节点的归档状态，并重新计算beginHeight和endHeight
	aStatus, err := cc.GetArchiveStatus()
	if err != nil {
		return err
	}
	if aStatus.Process != store.ArchiveProcess_Normal {
		return errors.New("peer archive is in process")
	}
	beginHeight := uint64(0)
	if aStatus.ArchivePivot > beginHeight {
		beginHeight = aStatus.ArchivePivot + 1
	}
	endHeight := archiveHeight
	if endHeight > aStatus.MaxAllowArchiveHeight {
		endHeight = aStatus.MaxAllowArchiveHeight
	}
	//2.检查归档服务的归档状态，并重新计算beginHeight
	archivedHeight, inArchive, code, err := cc.GetArchiveService().GetArchivedStatus()
	if err != nil {
		if strings.Contains(err.Error(), "chain genesis not exists") { //未注册，先注册
			genesis, err1 := cc.GetBlockByHeight(0, true)
			if err1 != nil {
				return err1
			}
			err1 = cc.GetArchiveService().Register(genesis)
			if err1 != nil {
				return err1
			}
		} else {
			cc.logger.Warnf("get archive status error:%s,code:%d", err, code)
			return err
		}
	}
	if inArchive {
		return errors.New("archive service is in process")
	}
	//3.中间有跳块，则报错
	if beginHeight > archivedHeight+1 {
		return fmt.Errorf("peer archive begin height:%d, archive service height:%d, not match",
			beginHeight, archivedHeight)
	}
	beginHeight = archivedHeight + 1

	//4.逐个查询区块，并归档
	var blockIter BlockIterator = &blocks{
		queryFunc:    cc.GetBlockByHeight,
		height:       beginHeight - 1, //因为迭代器Next的时候+1，所以这里先-1
		endHeight:    endHeight,
		total:        endHeight - beginHeight + 1, //左闭右闭区间，所以+1
		heightNotice: heightNotice,
	}
	cc.logger.Debugf("start process archive from %d to %d", beginHeight, endHeight)
	err = cc.GetArchiveService().ArchiveBlocks(blockIter, heightNotice)
	cc.logger.Debugf("archive from %d to %d complete", beginHeight, endHeight)
	if err != nil {
		return err
	}

	return nil
}

// blocks 区块查询迭代器实现
type blocks struct {
	queryFunc    func(blockHeight uint64, withRWSet bool) (*common.BlockInfo, error)
	height       uint64
	endHeight    uint64
	total        uint64
	heightNotice func(ProcessMessage) error
}

func (b *blocks) Next() bool {
	b.height++
	return b.height <= b.endHeight
}

func (b *blocks) Value() (*common.BlockInfo, error) {
	blk, err := b.queryFunc(b.height, true)
	//if b.heightNotice != nil {
	//	b.heightNotice(ProcessMessage{
	//		CurrentHeight: b.height,
	//		Total:         b.total,
	//		Error:         err,
	//	})
	//}
	return blk, err
}

func (b *blocks) Release() {

}

func (b *blocks) Total() uint64 {
	return b.total
}

func (b *blocks) Current() uint64 {
	return b.height
}

// isHeightInRestoreRange 判断一个区块高度是否已经在恢复过程中了
// @param height
// @param ranges
// @return bool
func isHeightInRestoreRange(height uint64, ranges []*store.FileRange) bool {
	for _, r := range ranges {
		if height >= r.Start && height <= r.End {
			return true
		}
	}
	return false
}

// RestoreBlocks 从归档服务查询已归档区块恢复到节点账本中
// @param endHeight
// @return error
func (cc *ChainClient) RestoreBlocks(restoreHeight uint64, _ string,
	heightNotice func(message ProcessMessage) error) error {
	archiveStatus, archiveStatusErr := cc.GetArchiveStatus()
	if archiveStatusErr != nil {
		return fmt.Errorf("GetArchiveStatus got error : %s ", archiveStatusErr.Error())
	}
	// step 0 check status is normal ,
	if archiveStatus.Process != store.ArchiveProcess_Normal {
		return fmt.Errorf("chain is restoreing or archiveing, retry later !"+
			"GetArchiveStatus got archiveProcess %d ", archiveStatus.Process)
	}
	if restoreHeight > archiveStatus.ArchivePivot { //恢复高度大于归档高度，无效恢复
		return fmt.Errorf("chain's archive pivot is %d, your restore height is %d, no block needs restore",
			archiveStatus.ArchivePivot, restoreHeight)
	}
	// step 1  查询从高度h开始传输区块 ,restoreEndHeight <= h
	beginHeight := archiveStatus.ArchivePivot
	endHeight := restoreHeight
	cc.logger.Debugf("start process restore from %d to %d", beginHeight, endHeight)
	//为了更好的兼容其他方式的归档恢复，所以我们从高的往低的恢复
	for h := beginHeight; h >= endHeight; h-- {
		if heightNotice != nil {
			err1 := heightNotice(ProcessMessage{
				CurrentHeight: h,
				Total:         beginHeight - endHeight + 1,
				Error:         nil,
			})
			if err1 != nil {
				return err1
			}
		}
		if isHeightInRestoreRange(h, archiveStatus.FileRanges) {
			continue
		}
		//step2 从归档服务查询区块
		blk, err := cc.GetArchiveService().GetBlockByHeight(h, true)
		if err != nil {
			return err
		}
		//step 3将区块构建Restore Payload，并发送给节点
		err = cc.sendRestoreBlockReq([]*common.BlockInfo{blk})
		if err != nil {
			return err
		}
		if h == 0 {
			break
		}
	}
	cc.logger.Debugf("restore from %d to %d complete", beginHeight, endHeight)
	return nil
}

func (cc *ChainClient) sendRestoreBlockReq(blocks []*common.BlockInfo) error {
	var (
		err     error
		payload *common.Payload
		resp    *common.TxResponse
	)
	fullBlocks := make([][]byte, len(blocks))
	for i, b := range blocks {
		fullBlocks[i], _ = b.Marshal()
	}
	payload, err = cc.CreateRestoreBlocksPayload(fullBlocks)
	if err != nil {
		return fmt.Errorf("CreateRestoreBlockPayload get error [%s]", err.Error())
	}

	resp, err = cc.SendRestoreBlockRequest(payload, -1)
	if err != nil {
		return fmt.Errorf("SendRestoreBlockRequest get error [%s]", err.Error())
	}
	if resp.Code == common.TxStatusCode_SUCCESS {
		return nil
	}
	return fmt.Errorf("SendRestoreBlockRequest fail, code:%d,message:%s", resp.Code, resp.Message)
}
