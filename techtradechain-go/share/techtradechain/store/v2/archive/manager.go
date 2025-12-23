/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0

*/

package archive

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"techtradechain.com/techtradechain/common/v2/crypto/hash"
	"techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/store/v2/binlog"
	"techtradechain.com/techtradechain/store/v2/blockdb"
	"techtradechain.com/techtradechain/store/v2/blockdb/blockhelper"
	"techtradechain.com/techtradechain/store/v2/conf"
	"techtradechain.com/techtradechain/store/v2/resultdb"
	"techtradechain.com/techtradechain/store/v2/resultdb/resulthelper"
	"techtradechain.com/techtradechain/store/v2/serialization"
	"techtradechain.com/techtradechain/store/v2/types"
	tbf "techtradechain.com/techtradechain/store/v2/types/blockfile"
	su "techtradechain.com/techtradechain/store/v2/utils"
	"techtradechain.com/techtradechain/utils/v2"
	"github.com/gogo/protobuf/proto"
)

const defaultMinUnArchiveBlockHeight = 10
const defaultUnArchiveBlockHeight = 300000 // about 7 days block produces
const defaultArchiveCheckInterval = 10     // default archive check interval: 10s
const defaultRestoreInterval = 60          // default restore interval: 60s

var (
	errHeightNotReach        = errors.New("target archive height not reach")
	errLastHeightTooLow      = errors.New("chain last height too low to archive")
	errHeightTooLow          = errors.New("target archive height too low")
	errRestoreHeightNotMatch = errors.New("restore block height not match last archived height")
)

// ArchiveMgr provide handle to archive instances
//
//	@Description:
type ArchiveMgr struct {
	sync.RWMutex
	archivedPivot         uint64
	unarchivedBlockHeight uint64
	blockDB               blockdb.BlockDB
	resultDB              resultdb.ResultDB
	fileStore             binlog.BinLogger
	fileRestore           *FileRestore
	storeConfig           *conf.StorageConfig
	batchPools            []*sync.Pool
	useFileDB             bool
	logger                protocol.Logger
	chainId               string
	bfdbPath              string
	hashType              string
	inProcessStatus       int32
	lastRestore           time.Time
	stopCh                chan struct{}
}

// NewArchiveMgr construct a new `archiveMgr` with given chainId
//
//	@Description:
//	@param chainId
//	@param blockDB
//	@param resultDB
//	@param storeConfig
//	@param logger
//	@return *archiveMgr
//	@return error
func NewArchiveMgr(chainId, bfdbPath string, blockDB blockdb.BlockDB, resultDB resultdb.ResultDB,
	fileStore binlog.BinLogger, storeConfig *conf.StorageConfig,
	logger protocol.Logger) (*ArchiveMgr, error) {
	archiveMgr := &ArchiveMgr{
		blockDB:         blockDB,
		resultDB:        resultDB,
		fileStore:       fileStore,
		logger:          logger,
		storeConfig:     storeConfig,
		batchPools:      make([]*sync.Pool, 0, 2),
		useFileDB:       !storeConfig.DisableBlockFileDb,
		chainId:         chainId,
		bfdbPath:        bfdbPath,
		lastRestore:     time.Now(),
		inProcessStatus: int32(storePb.ArchiveProcess_Normal),
		stopCh:          make(chan struct{}),
	}

	// create archiving and restoring dir
	if archiveMgr.useFileDB {
		var err error
		opts := DefaultOptions
		opts.NoSync = storeConfig.LogDBSegmentAsync
		opts.SegmentSize = storeConfig.LogDBSegmentSize * 1024 * 1024
		archiveMgr.fileRestore, err = OpenRestore(path.Join(bfdbPath, tbf.RestoringPath), opts, archiveMgr.logger)
		if err != nil {
			return nil, err
		}

		if storeConfig.ArchiveCheckInterval == 0 {
			storeConfig.ArchiveCheckInterval = defaultArchiveCheckInterval
		}
		if storeConfig.RestoreInterval == 0 {
			storeConfig.RestoreInterval = defaultRestoreInterval
		}

		if storeConfig.RestoreInterval < storeConfig.ArchiveCheckInterval {
			storeConfig.RestoreInterval = storeConfig.ArchiveCheckInterval * 2
		}

		// batchPools[0] for block archive update batch
		archiveMgr.batchPools = append(archiveMgr.batchPools, types.NewSyncPoolUpdateBatch())
		// batchPools[1] for result archive update batch
		archiveMgr.batchPools = append(archiveMgr.batchPools, types.NewSyncPoolUpdateBatch())

		if err = su.CreatePath(path.Join(archiveMgr.bfdbPath, tbf.ArchivingPath)); err != nil {
			return nil, err
		}

		go archiveMgr.moveFDBFiles()
	}

	unarchiveBlockHeight := uint64(0)
	cfgUnArchiveBlockHeight := archiveMgr.storeConfig.UnArchiveBlockHeight
	if cfgUnArchiveBlockHeight == 0 {
		unarchiveBlockHeight = defaultUnArchiveBlockHeight
		archiveMgr.logger.Infof(
			"config UnArchiveBlockHeight not set, will set to defaultMinUnArchiveBlockHeight:[%d]", defaultUnArchiveBlockHeight)
	} else if cfgUnArchiveBlockHeight <= defaultMinUnArchiveBlockHeight {
		unarchiveBlockHeight = defaultMinUnArchiveBlockHeight
		archiveMgr.logger.Infof(
			"config UnArchiveBlockHeight is too low:[%d], will set to defaultMinUnArchiveBlockHeight:[%d]",
			cfgUnArchiveBlockHeight, defaultMinUnArchiveBlockHeight)
	} else if cfgUnArchiveBlockHeight > defaultMinUnArchiveBlockHeight {
		unarchiveBlockHeight = cfgUnArchiveBlockHeight
	}

	archiveMgr.unarchivedBlockHeight = unarchiveBlockHeight
	if _, err := archiveMgr.GetArchivedPivot(); err != nil {
		return nil, err
	}

	return archiveMgr, nil
}

// ArchiveBlock archive block to target height
//
//	@Description:
//	@receiver mgr
//	@param archiveHeight
//	@return error
func (mgr *ArchiveMgr) ArchiveBlock(archiveHeight uint64) error {
	if !atomic.CompareAndSwapInt32(&mgr.inProcessStatus, int32(storePb.ArchiveProcess_Normal),
		int32(storePb.ArchiveProcess_Archiving)) {
		errStr := fmt.Sprintf("chain[%s] is archiving or restoring, please try again later", mgr.chainId)
		mgr.logger.Warn(errStr)
		return fmt.Errorf(errStr)
	}
	defer atomic.StoreInt32(&mgr.inProcessStatus, int32(storePb.ArchiveProcess_Normal)) // 归档

	mgr.Lock()
	defer mgr.Unlock()

	var (
		err                       error
		lastHeight, archivedPivot uint64
		txIdsMap                  map[uint64][]string
	)

	if lastHeight, err = mgr.blockDB.GetLastSavepoint(); err != nil {
		return err
	}
	if archivedPivot, err = mgr.GetArchivedPivot(); err != nil {
		return err
	}

	//archiveHeight should between archivedPivot and (lastHeight - unarchivedBlockHeight)
	if lastHeight <= mgr.unarchivedBlockHeight {
		return errLastHeightTooLow
	} else if mgr.archivedPivot >= archiveHeight {
		return errHeightTooLow
	} else if archiveHeight >= lastHeight-mgr.unarchivedBlockHeight {
		return errHeightNotReach
	}

	if mgr.useFileDB {
		start := archivedPivot + 1
		if archivedPivot == 0 {
			start = archivedPivot
		}
		return mgr.archiveFileDB(start, archiveHeight)
	}

	if txIdsMap, err = mgr.blockDB.ShrinkBlocks(archivedPivot+1, archiveHeight, mgr.bfdbPath); err != nil {
		return err
	}
	// 只有启用了resultdb,才去清理数据
	if !mgr.storeConfig.DisableResultDB {
		if err = mgr.resultDB.ShrinkBlocks(txIdsMap, 0, ""); err != nil {
			return err
		}
	}
	mgr.logger.Infof("archived block from [%d] to [%d], block size:%d",
		mgr.archivedPivot, archiveHeight, archiveHeight-mgr.archivedPivot)

	return nil
}

// archiveFileDB add next time
// @Description:
// @receiver mgr
// @param archivedPivot
// @param archiveHeight
// @return error
func (mgr *ArchiveMgr) archiveFileDB(archivedPivot, archiveHeight uint64) error {
	// 1. compute scan [beginStart , endStart]
	begin, end, err := mgr.calculateArchiveRange(archivedPivot, archiveHeight)
	if err != nil {
		return err
	}

	mgr.logger.Infof("there are block[%d~%d] (total: %d) rest blocks(cnt: %d) will be archived next time",
		end, begin, end-begin+1, archiveHeight-end)

	// 2. get config blocks
	confBlks, err1 := mgr.getConfigBlocks(begin, end)
	if err1 != nil {
		return err1
	}

	// 3. filter out shrink fdb files
	fileRngs, errfn := filterFileNames(begin, end, mgr.bfdbPath)
	if errfn != nil {
		return errfn
	}
	if len(fileRngs) == 0 {
		mgr.logger.Warnf("blocks[%d~%d] not need to shrink under bfdbPath: %s,", begin, end, mgr.bfdbPath)
		return nil
	}

	// 4. compute file [start,end] info
	fnConfBlks := make(map[string][]*storePb.BlockWithRWSet)
	for i := 0; i < len(fileRngs); i++ {
		confBlkM := make([]*storePb.BlockWithRWSet, 0)
		for j := 0; j < len(confBlks); j++ {
			if fileRngs[i].Start <= confBlks[j].Block.Header.BlockHeight {
				if fileRngs[i].End == 0 ||
					confBlks[j].Block.Header.BlockHeight <= fileRngs[i].End {
					confBlkM = append(confBlkM, confBlks[j])
				}
			}
		}
		if len(confBlkM) > 0 {
			fnConfBlks[fileRngs[i].FileName] = confBlkM
		}
	}
	for _, fileRng := range fileRngs {
		if err = mgr.archiveFDB(fileRng, fnConfBlks[fileRng.FileName]); err != nil {
			return err
		}
	}

	return nil
}

// archiveFDB add next time
// @Description:
// @receiver mgr
// @param fileName
// @param fileRange
// @param configBlks
// @return error
func (mgr *ArchiveMgr) archiveFDB(fileRange *storePb.FileRange, configBlks []*storePb.BlockWithRWSet) error {
	// 1. move config block save to kvdb
	dbType := mgr.blockDB.GetDbType()

	mgr.logger.Debugf("archive FDB file name: %s, start: %d, end: %d",
		fileRange.FileName, fileRange.Start, fileRange.End)

	if fileRange.Start > fileRange.End {
		return fmt.Errorf("invalidate archive file range, name: %s, start: %d, end: %d",
			fileRange.FileName, fileRange.Start, fileRange.End)
	}

	// get update batch sync.Pool
	arcBatchs := make([]*types.UpdateBatch, 0, len(mgr.batchPools))
	for i := 0; i < len(mgr.batchPools); i++ {
		batch, ok := mgr.batchPools[i].Get().(*types.UpdateBatch)
		if !ok {
			return fmt.Errorf("archive get %dst update batch failed", i)
		}
		batch.ReSet()
		arcBatchs = append(arcBatchs, batch)
	}

	if len(arcBatchs) < 2 {
		return fmt.Errorf("archive batch should more than 2 batches")
	}

	// build config block kv batch
	blkBatch, resBatch := arcBatchs[0], arcBatchs[1]
	for _, confBlk := range configBlks {
		_, blockInfo, errs := serialization.SerializeBlock(confBlk)
		if errs != nil {
			return errs
		}
		blockhelper.BuildKVBatch(true, blkBatch, blockInfo, dbType, mgr.logger)
		if !mgr.storeConfig.DisableResultDB {
			resulthelper.BuildKVBatch(resBatch, blockInfo, mgr.logger)
		}
	}
	// remove useless key
	blkBatch.Remove([]byte(blockhelper.LastBlockNumKeyStr))
	blkBatch.Remove([]byte(blockhelper.LastConfigBlockNumKey))
	if !mgr.storeConfig.DisableResultDB {
		resBatch.Remove([]byte(resulthelper.ResultDBSavepointKey))
	}

	// 2. delete xxx.fdb's block index exclude config block index
	for height := fileRange.Start; height <= fileRange.End; height++ {
		brw, errb := mgr.getBlockWithRWSets(height)
		if errb != nil {
			return errb
		}
		if brw == nil {
			continue
		}

		blkBatch.Delete(blockhelper.ConstructBlockIndexKey(dbType, height))
		blkBatch.Delete(blockhelper.ConstructBlockMetaIndexKey(dbType, height))
		header := brw.Block.Header
		for i, tx := range brw.Block.Txs {
			blkBatch.Put(blockhelper.ConstructBlockTxIDKey(tx.Payload.TxId),
				blockhelper.ConstructFBTxIDBlockInfo(header.BlockHeight, header.BlockHash, uint32(i),
					header.BlockTimestamp, nil, nil))
		}

		if !mgr.storeConfig.DisableResultDB {
			for _, rw := range brw.TxRWSets {
				resBatch.Delete(resulthelper.ConstructTxRWSetIndexKey(rw.TxId))
			}
		}
	}

	// 3. save batch to archiving file
	blkBatch.Put([]byte(blockhelper.ArchivedPivotKey), blockhelper.ConstructBlockNumKey(dbType, fileRange.End))

	// 4. write batch to file
	pairs := types.BatchToIndexPairs(blkBatch)
	batchBytes, errf := json.Marshal(pairs)
	if errf != nil {
		return errf
	}
	mgr.batchPools[0].Put(blkBatch)
	indexFN := path.Join(mgr.bfdbPath, tbf.ArchivingPath,
		fmt.Sprintf("%s%s", fileRange.FileName, tbf.ArchivingBlockFileName))
	if errw := os.WriteFile(indexFN, batchBytes, os.FileMode(0666)); errw != nil {
		return errw
	}
	if !mgr.storeConfig.DisableResultDB {
		pairs = types.BatchToIndexPairs(resBatch)
		batchBytes, errf = json.Marshal(pairs)
		if errf != nil {
			return errf
		}
		indexFN = path.Join(mgr.bfdbPath, tbf.ArchivingPath,
			fmt.Sprintf("%s%s", fileRange.FileName, tbf.ArchivingResultFileName))
		if errw := os.WriteFile(indexFN, batchBytes, os.FileMode(0666)); errw != nil {
			return errw
		}
	}
	mgr.batchPools[1].Put(resBatch)
	return nil
}

// getBlockWithRWSets add next time
// @Description:
// @receiver mgr
// @param height
// @return *storePb.BlockWithRWSet
// @return error
func (mgr *ArchiveMgr) getBlockWithRWSets(height uint64) (*storePb.BlockWithRWSet, error) {
	index, errb := mgr.blockDB.GetBlockIndex(height)
	if errb != nil {
		return nil, errb
	}
	if index == nil {
		return nil, nil
	}
	data, errb := mgr.fileStore.ReadFileSection(index, 0)
	if errb != nil {
		return nil, errb
	}
	brw, errb := serialization.DeserializeBlock(data)
	if errb != nil {
		mgr.logger.Warnf("get block[%d] from file deserialize block failed: %v", height, errb)
		return nil, errb
	}
	return brw, nil
}

// RestoreBlocks restore block from outside block data
//
//	@Description:
//	@receiver mgr
//	@param blockInfos
//	@return error
func (mgr *ArchiveMgr) RestoreBlocks(serializedBlocks [][]byte) error {
	if !atomic.CompareAndSwapInt32(&mgr.inProcessStatus, int32(storePb.ArchiveProcess_Normal),
		int32(storePb.ArchiveProcess_Restoring)) {
		msg := fmt.Sprintf("chain [%s] is archiving or restoring, please try again later", mgr.chainId)
		mgr.logger.Errorf(msg)
		return fmt.Errorf(msg)
	}

	defer atomic.StoreInt32(&mgr.inProcessStatus, int32(storePb.ArchiveProcess_Normal)) // 复原

	if len(serializedBlocks) == 0 {
		mgr.logger.Warnf("restore block is empty")
		return nil
	}

	if err := mgr.getHashType(); err != nil {
		mgr.logger.Warnf("get archive mgr crypto hash type failed: %v", err)
	}

	ap, err := mgr.GetArchivedPivot()
	if err != nil {
		return err
	}
	if ap == 0 && mgr.blockDB.IsArchivedPivotKeyZeroStatus() {
		mgr.logger.Warnf("archive pivot is 0, do not need restore")
		return nil
	}

	vBytes := make([][]byte, 0, len(serializedBlocks))
	blockInfos := make([]*serialization.BlockWithSerializedInfo, 0, len(serializedBlocks))
	for _, blockInfo := range serializedBlocks {
		bwsInfo, errb := serialization.DeserializeBlock(blockInfo) // Add sync.Pool
		if errb != nil {
			return errb
		}
		vByte, s, errb := serialization.SerializeBlock(bwsInfo)
		if errb != nil {
			return errb
		}

		errb = mgr.verifyBlock(bwsInfo)
		if errb != nil {
			return fmt.Errorf("block[%d] verify failed: %v", bwsInfo.Block.Header.BlockHeight, errb)
		}

		blockInfos = append(blockInfos, s)
		vBytes = append(vBytes, vByte)
	}

	// restore block info should be in ascending continuous order
	total := len(blockInfos)
	curHeight := blockInfos[total-1].Block.Header.BlockHeight
	for i := 0; i < total; i++ {
		if blockInfos[total-i-1].Block.Header.BlockHeight != curHeight {
			return tbf.ErrInvalidateRestoreBlocks
		}
		curHeight = curHeight - 1
	}

	if mgr.useFileDB {
		return mgr.RestoreFileDB(vBytes, blockInfos)
	}

	return mgr.RestoreKvDB(blockInfos)
}

// moveFDBFiles move FDB files to bfdb by archive and restore action
// @Description:
// @receiver mgr
func (mgr *ArchiveMgr) moveFDBFiles() {
	var err error
	interval := time.Second * time.Duration(mgr.storeConfig.ArchiveCheckInterval)

	for {
		select {
		case <-mgr.stopCh:
			mgr.logger.Infof("archive mgr move FDB files action closed")
			return
		default:
			if err = mgr.doArchive(); err != nil {
				mgr.logger.Warnf("do archive failed, err: %v", err)
			}
			if err = mgr.doRestore(); err != nil {
				mgr.logger.Warnf("do restore failed, err: %v", err)
			}
			time.Sleep(interval)
		}
	}
}

// doArchive add next time
// @Description:
// @receiver mgr
// @return error
func (mgr *ArchiveMgr) doArchive() error {
	if !atomic.CompareAndSwapInt32(&mgr.inProcessStatus, int32(storePb.ArchiveProcess_Normal),
		int32(storePb.ArchiveProcess_Archiving)) {
		return nil
	}
	defer atomic.StoreInt32(&mgr.inProcessStatus, int32(storePb.ArchiveProcess_Normal))

	bNums, rNums, err := mgr.scanArchivingDir()
	if err != nil {
		return err
	}
	if len(bNums) == 0 && len(rNums) == 0 {
		return nil
	}

	if len(rNums)-len(bNums) > 1 {
		return fmt.Errorf("archive block and result height distance should less then 1")
	}

	if _, err = mgr.GetArchivedPivot(); err != nil {
		mgr.logger.Warnf("update archive pivot failed, err: %v", err)
	}

	// normal bNums data: archivePivot < bNums[0] < bNums[n] < targetArchivePivot < unarchivedBlockHeight
	// archive is uncompleted last time
	if len(rNums)-len(bNums) == 1 {
		if errs := mgr.resultDB.ShrinkBlocks(nil, rNums[0], mgr.bfdbPath); errs != nil {
			mgr.logger.Errorf("archive mgr shrink result err: %v", errs)
			return errs
		}
		// delete archived index file
		fdbPath := path.Join(mgr.bfdbPath, fmt.Sprintf("%s%s", tbf.Uint64ToSegmentName(rNums[0]), tbf.DBFileSuffix))
		if errs := os.RemoveAll(fdbPath); errs != nil {
			mgr.logger.Errorf("archive mgr remove %s err: %v", fdbPath, errs)
			return errs
		}
	}

	for _, height := range bNums {
		if _, errs := mgr.blockDB.ShrinkBlocks(height, 0, mgr.bfdbPath); errs != nil {
			mgr.logger.Errorf("archive mgr shrink block err: %v", errs)
			return errs
		}
		if errs := mgr.resultDB.ShrinkBlocks(nil, height, mgr.bfdbPath); errs != nil {
			mgr.logger.Errorf("archive mgr shrink result err: %v", errs)
			return errs
		}

		// delete archived index file
		fdbPath := path.Join(mgr.bfdbPath, fmt.Sprintf("%s%s", tbf.Uint64ToSegmentName(height), tbf.DBFileSuffix))
		if errs := os.RemoveAll(fdbPath); errs != nil {
			mgr.logger.Errorf("archive mgr remove %s err: %v", fdbPath, errs)
			return errs
		}
		mgr.fileStore.DropOpenedFileCache(fdbPath)
		mgr.logger.Infof("shrink %s.fdb data finished", tbf.Uint64ToSegmentName(height))
	}

	if len(bNums) > 0 {
		go func() {
			_ = mgr.blockDB.CompactRange()
			_, _ = mgr.GetArchivedPivot()
		}()
	}

	return nil
}

// scanArchivingDir scan bfdb/archiving dir
// @Description:
// @receiver mgr
// @return []uint64
// @return []uint64
// @return error
func (mgr *ArchiveMgr) scanArchivingDir() ([]uint64, []uint64, error) {
	archivePath := path.Join(mgr.bfdbPath, tbf.ArchivingPath)
	fis, err := ioutil.ReadDir(archivePath)
	if err != nil || len(fis) == 0 {
		return nil, nil, err
	}

	bNums := make([]uint64, 0, len(fis))
	rNums := make([]uint64, 0, len(fis))

	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		name := fi.Name()
		if !tbf.CheckSegmentName(name) {
			continue
		}

		// get xxx.block.archiving.json file
		if strings.HasSuffix(name, tbf.ArchivingBlockFileName) {
			bNum, err1 := tbf.SegmentNameToUint64(name[:len(tbf.ArchivingBlockFileName)])
			if err1 != nil {
				return nil, nil, fmt.Errorf("file name: %s invalidate, err: %v", name, err1)
			}
			bNums = append(bNums, bNum)
			continue
		}

		// get xxx.result.archiving.json file
		if strings.HasSuffix(name, tbf.ArchivingResultFileName) {
			rNum, err1 := tbf.SegmentNameToUint64(name[:len(tbf.ArchivingResultFileName)])
			if err1 != nil {
				return nil, nil, fmt.Errorf("file name: %s invalidate, err: %v", name, err1)
			}
			rNums = append(rNums, rNum)
			continue
		}
	}

	// bNum is increase because of archive block height is increase
	sort.Slice(bNums, func(i, j int) bool {
		return bNums[i] < bNums[j]
	})

	// rNum is increase because of archive block height is increase
	sort.Slice(rNums, func(i, j int) bool {
		return rNums[i] < rNums[j]
	})

	return bNums, rNums, nil
}

// doRestore save .fdb to bfdb
// @Description:
// @receiver mgr
// @return error
func (mgr *ArchiveMgr) doRestore() error {
	if time.Since(mgr.lastRestore) <= time.Duration(mgr.storeConfig.RestoreInterval)*time.Second {
		return nil
	}
	if !atomic.CompareAndSwapInt32(&mgr.inProcessStatus, int32(storePb.ArchiveProcess_Normal),
		int32(storePb.ArchiveProcess_Restoring)) {
		return nil
	}
	defer atomic.StoreInt32(&mgr.inProcessStatus, int32(storePb.ArchiveProcess_Normal))

	//merge continues xxx.fdb
	if err := mgr.fileRestore.mergeSegment(); err != nil {
		return err
	}

	sPool := sync.Pool{}
	sPool.New = func() interface{} {
		return serialization.NewBlockSerializedInfo()
	}
	lSeg := mgr.fileRestore.LastSegment()
	if lSeg == nil {
		return nil
	}

	lastRng, segInfos, errl := mgr.readLastRange(lSeg)
	if errl != nil || lastRng == nil {
		return errl
	}

	if _, err := mgr.GetArchivedPivot(); err != nil {
		mgr.logger.Warnf("update archive pivot failed, err: %v", err)
	}
	// last range has restored but shutdown before index remove action,
	// we should remove the index and .fdb file and skip this file restore action
	//if mgr.archivedPivot == 0 || (lastRng.Start > 1 && lastRng.Start-1 == mgr.archivedPivot) {
	if mgr.blockDB.IsArchivedPivotKeyZeroStatus() || (lastRng.Start > 1 && lastRng.Start-1 == mgr.archivedPivot) {
		if err := mgr.flushToBFDB(lastRng); err != nil {
			return err
		}
		if err := os.RemoveAll(tbf.SegmentIndexPath(lastRng.FileName, mgr.fileRestore.path)); err != nil {
			return err
		}
		mgr.fileRestore.delSegmentByName(lastRng.FileName)
		mgr.logger.Infof("skip restored index and xxx.fdb file: %s, start: %d, end: %d",
			lastRng.FileName, lastRng.Start, lastRng.End)
		return nil
	}

	if lastRng.End != mgr.archivedPivot {
		mgr.logger.Infof("restoring last .fdb end[%d] not reach archivePivot[%d], will doRestore next time",
			lastRng.End, mgr.archivedPivot)
		return nil
	}

	// restore files this time
	// 1. move (copy than remove) xxx.fdb from restring dir to bfdb dir
	if err := mgr.flushToBFDB(lastRng); err != nil {
		return err
	}
	// 2. write index data to block and result kvDB
	if err := mgr.flushKVToDB(lastRng, segInfos, &sPool); err != nil {
		return err
	}
	if err := os.RemoveAll(tbf.SegmentIndexPath(lastRng.FileName, mgr.fileRestore.path)); err != nil {
		return err
	}

	mgr.fileRestore.delSegmentByName(lastRng.FileName)
	_, _ = mgr.GetArchivedPivot()
	return nil
}

// flushToBFDB restore .fdb to bfdb
// @Description:
// @receiver mgr
// @param lastRng
// @return error
func (mgr *ArchiveMgr) flushToBFDB(lastRng *storePb.FileRange) error {
	oldPath := tbf.SegmentPath(lastRng.FileName, mgr.fileRestore.path)
	exists, errX := su.PathExists(oldPath)
	if !exists {
		if errX != nil {
			return fmt.Errorf("fdb:%s is unavailable: %v", oldPath, errX)
		}
		mgr.logger.Warnf("fdb:%s is not exists maybe caused by uncompleted index write previous time")
	} else {
		if errX = mgr.fileRestore.closeSegmentByName(lastRng.FileName); errX != nil {
			return errX
		}
		newPath := tbf.SegmentPath(lastRng.FileName, mgr.bfdbPath)
		if errX = tbf.CopyThenRemoveFile(oldPath, newPath, nil); errX != nil {
			return errX
		}
	}
	return nil
}

// ResumeFlushKV resume flush kv to db
// Deprecated
// @Description:
// @receiver mgr
// @param uIndexSN
// @param sPool
// @return error
func (mgr *ArchiveMgr) ResumeFlushKV(uIndexSN []string, sPool *sync.Pool) error {
	// sort uIndexSN in ascending order
	if len(uIndexSN) == 0 {
		return nil
	}

	sort.Slice(uIndexSN, func(i, j int) bool {
		return uIndexSN[i] < uIndexSN[j]
	})

	var (
		err      error
		segInfos []*tbf.SegIndex
		lastRng  *storePb.FileRange
	)
	for _, isn := range uIndexSN {
		indexPath := tbf.SegmentIndexPath(mgr.fileRestore.path, isn)
		fdbPath := tbf.SegmentPath(mgr.bfdbPath, isn)
		if exists, _ := su.PathExists(fdbPath); !exists {
			mgr.logger.Infof("index: %s exists, but fdb:%s is not exists, "+
				"will abandon this index file, please restore this part again", indexPath, fdbPath)
			continue
		}

		if lastRng, segInfos, err = mgr.readLastRange(&Segment{name: isn}); err != nil {
			return err
		}
		for i := lastRng.End; i >= lastRng.Start; i-- {
			bIndex := segInfos[uint64(len(segInfos))-(lastRng.End-i)-1].BIndex
			mgr.logger.Infof("restore block: %d index", bIndex.Height)
			if err = mgr.restoreRange(sPool, lastRng, bIndex); err != nil {
				return err
			}
			if i == 0 {
				break
			}
		}
		if err = os.RemoveAll(indexPath); err != nil {
			return err
		}
	}
	return err
}

// flushKVToDB flush kv to db
// @Description:
// @receiver mgr
// @param lastRng
// @param segInfos
// @param sPool
// @return error
func (mgr *ArchiveMgr) flushKVToDB(lastRng *storePb.FileRange, segInfos []*tbf.SegIndex, sPool *sync.Pool) error {
	var err error
	for i := lastRng.End; i >= lastRng.Start; i-- {
		bIndex := segInfos[uint64(len(segInfos))-(lastRng.End-i)-1].BIndex
		mgr.logger.Infof("restore block: %d index", bIndex.Height)
		if err = mgr.restoreRange(sPool, lastRng, bIndex); err != nil {
			return err
		}
		if i == 0 {
			break
		}
	}
	return err
}

// readLastRange find last range for restore
// @Description:
// @receiver mgr
// @param segInfos
// @param lastRng
// @param indexF
// @return error
// @return []*tbf.SegIndex
// @return string
// @return error
func (mgr *ArchiveMgr) readLastRange(seg *Segment) (*storePb.FileRange, []*tbf.SegIndex, error) {
	segInfos := seg.segIndexs
	if len(segInfos) == 0 {
		fw, err := tbf.NewFileWriter(tbf.SegmentIndexPath(seg.name, mgr.fileRestore.path))
		if err != nil {
			return nil, nil, err
		}
		if segInfos, err = tbf.LoadArchiveSegIndex(fw); err != nil {
			return nil, nil, err
		}
	}
	if len(segInfos) == 0 {
		return nil, nil, nil
	}

	lastRng := &storePb.FileRange{
		FileName: seg.name,
		Start:    segInfos[0].Height,
		End:      segInfos[len(segInfos)-1].Height,
	}
	return lastRng, segInfos, nil
}

//// findLastRange find last range for restore
//// @Description:
//// Deprecated
//// @receiver mgr
//// @param segInfos
//// @param lastRng
//// @param indexF
//// @return error
//// @return []*tbf.SegIndex
//// @return string
//// @return error
//func (mgr *ArchiveMgr) findLastRange() (string, *storePb.FileRange, []*tbf.SegIndex, error) {
//	lastRng := &storePb.FileRange{}
//	indexF := ""
//	segInfos := make([]*tbf.SegIndex, 0)
//
//	restoringPath := path.Join(mgr.bfdbPath, tbf.RestoringPath)
//	fis, err := ioutil.ReadDir(restoringPath)
//	if err != nil || len(fis) == 0 {
//		return "", nil, nil, err
//	}
//
//	if _, err = mgr.GetArchivedPivot(); err != nil {
//		mgr.logger.Warnf("update archive pivot failed, err: %v", err)
//	}
//
//	for _, fi := range fis {
//		if fi.IsDir() {
//			continue
//		}
//
//		name := fi.Name()
//		if !tbf.CheckSegmentName(name) {
//			continue
//		}
//
//		// get xxx.restoring.json file
//		if !strings.HasSuffix(name, tbf.RestoringIndexFileName) ||
//			len(name) != len(tbf.RestoringIndexFileName)+tbf.DBFileNameLen {
//			continue
//		}
//
//		fw, errf := tbf.NewFileWriter(path.Join(restoringPath, name))
//		if errf != nil {
//			return "", nil, nil, err
//		}
//		segInfos, err = tbf.LoadArchiveSegIndex(fw)
//		if err != nil {
//			return "", nil, nil, err
//		}
//
//		if len(segInfos) == 0 {
//			continue
//		}
//		if segInfos[0].Height > lastRng.Start || segInfos[0].Height == 0 {
//			lastRng.FileName = name
//			lastRng.Start = segInfos[0].Height
//			lastRng.End = segInfos[len(segInfos)-1].Height
//			indexF = name
//		}
//	}
//	return indexF, lastRng, segInfos, nil
//}

// RestoreFileDB add next time
// @Description:
// @receiver mgr
// @param blkBytes
// @param blockInfos
// @return error
func (mgr *ArchiveMgr) RestoreFileDB(blkBytes [][]byte, blockInfos []*serialization.BlockWithSerializedInfo) error {
	mgr.lastRestore = time.Now()
	// verify incoming restore block
	canSave, err := mgr.checkFBRestoreHeight(blockInfos)
	if err != nil {
		return err
	}
	if !canSave {
		return errRestoreHeightNotMatch
	}

	// save restored block to restoring dir
	for i := 0; i < len(blockInfos); i++ {
		brw := blockInfos[i]
		height := brw.Block.Header.BlockHeight
		indexMeta := &serialization.BlockIndexMeta{
			Height:         height,
			BlockHash:      brw.Block.Header.BlockHash,
			BlockTimestamp: brw.Block.Header.BlockTimestamp,
			TxIds:          brw.Meta.TxIds,
			RwSets:         make([]string, len(brw.TxRWSets)),
		}
		indexMeta.MetaIndex = brw.MetaIndex
		indexMeta.TxsIndex = brw.TxsIndex
		indexMeta.RWSetsIndex = brw.RWSetsIndex
		for k := 0; k < len(brw.SerializedTxRWSets); k++ {
			indexMeta.RwSets[k] = brw.TxRWSets[k].TxId
		}
		_, err = mgr.fileRestore.Write(height+1, blkBytes[i], indexMeta)
		if err != nil {
			return err
		}
	}

	return nil
}

// RestoreKvDB add next time
// @Description:
// @receiver mgr
// @param blockInfos
// @return error
func (mgr *ArchiveMgr) RestoreKvDB(blockInfos []*serialization.BlockWithSerializedInfo) error {
	if _, err := mgr.GetArchivedPivot(); err != nil {
		return err
	}

	// verify block height
	total := len(blockInfos)
	maxRHeight := blockInfos[total-1].Block.Header.BlockHeight
	if maxRHeight != mgr.archivedPivot {
		if cblk, _ := mgr.blockDB.GetBlock(maxRHeight); cblk != nil && utils.IsConfBlock(cblk) {
			mgr.logger.Infof("restore last block height[%d] is conf block, will skip it",
				blockInfos[total-1].Block.Header.BlockHeight)
			return nil
		}
		mgr.logger.Errorf("restore last block height[%d] not match node archived height[%d]",
			blockInfos[total-1].Block.Header.BlockHeight, mgr.archivedPivot)
		return errRestoreHeightNotMatch
	}

	// do restore
	if err := mgr.blockDB.RestoreBlocks(blockInfos); err != nil {
		return err
	}
	if !mgr.storeConfig.DisableResultDB {
		if err := mgr.resultDB.RestoreBlocks(blockInfos); err != nil {
			return err
		}
	}
	mgr.logger.Infof("restore block from [%d] to [%d], block size:%d",
		maxRHeight, blockInfos[0].Block.Header.BlockHeight, len(blockInfos))
	return nil
}

// GetArchivedPivot return restore block pivot
//
//	@Description:
//	@receiver mgr
//	@return uint64
//	@return error
func (mgr *ArchiveMgr) GetArchivedPivot() (uint64, error) {
	archivedPivot, err := mgr.blockDB.GetArchivedPivot()
	if err != nil {
		return 0, err
	}

	mgr.archivedPivot = archivedPivot
	return mgr.archivedPivot, nil
}

// GetArchiveStatus return archive status
//
//	@Description:
//	@receiver mgr
//	@return uint64
//	@return error
func (mgr *ArchiveMgr) GetArchiveStatus() (*storePb.ArchiveStatus, error) {
	archivedPivot, err := mgr.blockDB.GetArchivedPivot()
	if err != nil {
		return nil, err
	}

	lsp, errl := mgr.blockDB.GetLastSavepoint()
	if errl != nil {
		return nil, errl
	}

	if lsp == 0 {
		return nil, fmt.Errorf("block db is empty")
	}

	mgr.archivedPivot = archivedPivot
	as := &storePb.ArchiveStatus{
		Type:                  storePb.StoreType_RawDB,
		ArchivePivot:          archivedPivot,
		MaxAllowArchiveHeight: uint64(0),
		Process:               storePb.ArchiveProcess(mgr.inProcessStatus),
	}

	if lsp > mgr.unarchivedBlockHeight {
		as.MaxAllowArchiveHeight = lsp - mgr.unarchivedBlockHeight
	}

	if mgr.useFileDB {
		as.Type = storePb.StoreType_BFDB
		as.FileRanges = mgr.fileRestore.GetSegsInfo()
	}
	return as, nil
}

// checkFBRestoreHeight add next time
// @Description:
// @receiver mgr
// @param blockInfos
// @return bool
// @return error
func (mgr *ArchiveMgr) checkFBRestoreHeight(blockInfos []*serialization.BlockWithSerializedInfo) (bool, error) {
	as, err := mgr.GetArchiveStatus()
	if err != nil {
		return false, err
	}

	total := len(blockInfos)
	minRHeight := blockInfos[0].Block.Header.BlockHeight
	maxRHeight := blockInfos[total-1].Block.Header.BlockHeight
	if len(as.FileRanges) == 0 {
		if maxRHeight > as.ArchivePivot {
			mgr.logger.Errorf("restore last block height[%d] not match node archived height[%d]",
				maxRHeight, mgr.archivedPivot)
			return false, errRestoreHeightNotMatch
		}
		return true, nil
	}

	conflict := false
	for _, fr := range as.FileRanges {
		if fr.Start <= minRHeight && minRHeight <= fr.End ||
			fr.Start <= maxRHeight && maxRHeight <= fr.End {
			conflict = true
			break
		}
	}

	return !conflict, nil
}

// restoreRange add next time
// @Description:
// @receiver mgr
// @param sPool
// @param segInfos
// @param bIndex
// @return error
func (mgr *ArchiveMgr) restoreRange(sPool *sync.Pool, lastRng *storePb.FileRange,
	bIndex *serialization.BlockIndexMeta) error {
	blkInfo, ok := sPool.Get().(*serialization.BlockWithSerializedInfo)
	if !ok {
		return fmt.Errorf("get blkInfo sync.Pool failed")
	}
	blkInfo.ReSet()

	blkInfo.Meta = &storePb.SerializedBlock{
		Header: &common.BlockHeader{
			BlockHeight:    bIndex.Height,
			BlockHash:      bIndex.BlockHash,
			BlockTimestamp: bIndex.BlockTimestamp,
		},
		TxIds: bIndex.TxIds,
	}
	blkInfo.Index = bIndex.Index
	blkInfo.MetaIndex = bIndex.MetaIndex
	blkInfo.TxsIndex = bIndex.TxsIndex
	blkInfo.RWSetsIndex = bIndex.RWSetsIndex
	blkInfo.TxRWSets = make([]*common.TxRWSet, len(bIndex.RwSets))
	for k, txId := range bIndex.RwSets {
		blkInfo.TxRWSets[k] = &common.TxRWSet{TxId: txId}
	}

	if errs := mgr.blockDB.RestoreBlocks([]*serialization.BlockWithSerializedInfo{blkInfo}); errs != nil {
		mgr.logger.Errorf("archive mgr restore block[%d~%d] err: %v",
			lastRng.End, lastRng.Start, errs)
		return errs
	}
	if errs := mgr.resultDB.RestoreBlocks([]*serialization.BlockWithSerializedInfo{blkInfo}); errs != nil {
		mgr.logger.Errorf("archive mgr restore result[%d~%d] err: %v",
			lastRng.End, lastRng.Start, errs)
		return errs
	}

	sPool.Put(blkInfo)

	return nil
}

// calculateArchiveRange calculate archive block range by input (start, end)
// @Description:
// @receiver mgr
// @param start
// @param end
// @return uint64
// @return uint64
// @return error
func (mgr *ArchiveMgr) calculateArchiveRange(start, end uint64) (uint64, uint64, error) {
	var (
		err                     error
		sIndex, eIndex, e1Index *storePb.StoreInfo
		sNum, eNum, e1Num       uint64
	)

	fErrMsg := "get block index file name format invalidate: %s, err: %v"
	sIndex, err = mgr.blockDB.GetBlockIndex(start)
	if err != nil {
		return 0, 0, err
	}
	eIndex, err = mgr.blockDB.GetBlockIndex(end)
	if err != nil {
		return 0, 0, err
	}

	e1Index, err = mgr.blockDB.GetBlockIndex(end + 1)
	if err != nil {
		return 0, 0, err
	}

	sNum, err = tbf.SegmentNameToUint64(sIndex.GetFileName())
	if err != nil {
		return 0, 0, fmt.Errorf(fErrMsg, sIndex.GetFileName(), err)
	}

	// archive range start on .fdb file name
	if start >= sNum {
		start = sNum
	}

	// 1. archive range less than the .fdb contains, archive .fdb count should >= 1
	if sIndex.FileName == e1Index.FileName {
		return 0, 0, fmt.Errorf("archive range less than %s.fdb contains", sIndex.FileName)
	}

	// 2. archive range start and end both in one .fdb contains, and end pivot is .fdb last block height
	if sIndex.FileName == eIndex.FileName && eIndex.FileName != e1Index.FileName {
		return start, end, nil
	}

	// 3. archive range start and end in more than one .fdb contains, but end pivot is not .fdb last block height
	if sIndex.FileName != eIndex.FileName && eIndex.FileName == e1Index.FileName {
		eNum, err = tbf.SegmentNameToUint64(eIndex.GetFileName())
		if err != nil {
			return 0, 0, fmt.Errorf(fErrMsg, sIndex.GetFileName(), err)
		}

		if eNum <= end+1 {
			return start, eNum - 2, nil
		}
	}

	// 4. archive range start and end in more than one .fdb contains and end pivot is .fdb last block height
	e1Num, err = tbf.SegmentNameToUint64(e1Index.GetFileName())
	if err != nil {
		return 0, 0, fmt.Errorf(fErrMsg, sIndex.GetFileName(), err)
	}

	if e1Num == end+2 {
		return start, end, nil
	}

	return 0, 0, fmt.Errorf("caculate archive range[%d~%d] failed", start, end)
}

// getConfigBlocks 获取区块高度范围内所有的配置区块
// @Description:
// @param scanBeginHeight
// @param scanEndHeight
// @return []*common.Block
// @return error
func (mgr *ArchiveMgr) getConfigBlocks(begin, end uint64) ([]*storePb.BlockWithRWSet, error) {
	var (
		err error
		brw *storePb.BlockWithRWSet
	)

	confHeight := end
	confBlks := make([]*storePb.BlockWithRWSet, 0, 10)
	for err == nil && confHeight >= begin {
		brw, err = mgr.getBlockWithRWSets(confHeight)
		if err != nil {
			return nil, err
		}
		if brw == nil {
			return nil, fmt.Errorf("config block: %d should not empty", confHeight)
		}

		if utils.IsConfBlock(brw.Block) {
			confBlks = append(confBlks, brw)
			if confHeight == 0 {
				break
			}
		}
		confHeight = brw.Block.Header.PreConfHeight
	}

	sort.Slice(confBlks, func(i, j int) bool {
		return confBlks[i].Block.Header.BlockHeight > confBlks[j].Block.Header.BlockHeight
	})
	mgr.logger.Infof("get total %d config-blocks between height %d and %d", len(confBlks), begin, end)
	return confBlks, nil
}

// verifyBlock verify restore block
// @Description:
// @receiver mgr
// @param blk
// @return error
func (mgr *ArchiveMgr) verifyBlock(brw *storePb.BlockWithRWSet) error {
	if len(mgr.hashType) == 0 {
		mgr.logger.Warnf("archive mgr crypto hash is empty, skip block verify this time")
		return nil
	}

	if err := mgr.verifyHeader(brw.Block); err != nil {
		return err
	}

	if err := mgr.verifyTxRoot(brw.Block); err != nil {
		return err
	}

	if !mgr.storeConfig.DisableResultDB {
		if err := mgr.verifyRwSet(brw); err != nil {
			return err
		}
	}

	return mgr.verifyDagHash(brw.Block)
}

// verifyHeader add next time
// @Description:
// @receiver mgr
// @param blk
// @return error
// @return error
func (mgr *ArchiveMgr) verifyHeader(blk *common.Block) error {
	blkHash, err := utils.CalcBlockHash(mgr.hashType, blk)
	if err != nil {
		return err
	}

	if !bytes.Equal(blk.Header.BlockHash, blkHash) {
		return fmt.Errorf("restore block hash is invalidate: header.BlockHash: %s, calcBlockHash: %s",
			hex.EncodeToString(blk.Header.BlockHash), hex.EncodeToString(blkHash))
	}

	vHeight, errb := mgr.blockDB.GetHeightByHash(blk.Header.BlockHash)
	if errb != nil {
		return errb
	}

	if blk.Header.BlockHeight > 0 && vHeight != blk.Header.BlockHeight {
		return fmt.Errorf("restore block height is invalidate: header.BlockHeight: %d, db block height: %d",
			blk.Header.BlockHeight, vHeight)
	}
	return nil
}

// verifyTxRoot verify tx merkle root
// @Description:
// @receiver mgr
// @param blk
// @return error
func (mgr *ArchiveMgr) verifyTxRoot(blk *common.Block) error {
	bv := int(blk.Header.BlockVersion)
	txHashes := make([][]byte, 0, len(blk.Txs))
	for _, tx := range blk.Txs {
		txHash, err := utils.CalcTxHashWithVersion(mgr.hashType, tx, bv)
		if err != nil {
			return err
		}
		txHashes = append(txHashes, txHash)
	}

	root, err := hash.GetMerkleRoot(mgr.hashType, txHashes)
	if err != nil {
		return err
	}
	if !bytes.Equal(root, blk.Header.TxRoot) {
		return fmt.Errorf("block txRoot mismatch: blk.Header.TxRoot: %x, calculated: %x",
			blk.Header.TxRoot, root)
	}
	return nil
}

// verifyRwSet verify rwSets merkle root
// @Description:
// @receiver mgr
// @param brw
// @param blk
// @return error
func (mgr *ArchiveMgr) verifyRwSet(brw *storePb.BlockWithRWSet) error {
	rwSets := brw.TxRWSets
	if len(rwSets) == 0 {
		for _, tx := range brw.Block.Txs {
			rwSets = append(rwSets, &common.TxRWSet{
				TxId:     tx.Payload.TxId,
				TxReads:  nil,
				TxWrites: nil,
			})
		}
	}

	rwSetsH := make([][]byte, 0, len(rwSets))
	for _, rw := range rwSets {
		rwSetH, err := utils.CalcRWSetHash(mgr.hashType, rw)
		if err != nil {
			return err
		}
		rwSetsH = append(rwSetsH, rwSetH)
	}

	// verify rwSet merkle root
	root, err := hash.GetMerkleRoot(mgr.hashType, rwSetsH)
	if err != nil {
		return err
	}
	if !bytes.Equal(root, brw.Block.Header.RwSetRoot) {
		return fmt.Errorf("block rwSetRoot mismatch: blk.Header.RwSetRoot: %x, calculated: %x",
			brw.Block.Header.RwSetRoot, root)
	}
	return nil
}

// verifyDagHash verify dag hash
// @Description:
// @receiver mgr
// @param blk
// @return error
func (mgr *ArchiveMgr) verifyDagHash(blk *common.Block) error {
	// verify dag hash
	h, err := utils.CalcDagHash(mgr.hashType, blk.Dag)
	if err != nil {
		return err
	}
	if !bytes.Equal(h, blk.Header.DagHash) {
		return fmt.Errorf("block dagHash mismatch: blk.Header.DagHash: %x, calculated: %x",
			blk.Header.DagHash, h)
	}
	return nil
}

// getHashType add next time
// @Description:
// @receiver mgr
// @return error
func (mgr *ArchiveMgr) getHashType() error {
	if len(mgr.hashType) > 0 {
		return nil
	}

	lcb, err := mgr.blockDB.GetLastConfigBlock()
	if err != nil {
		return err
	}

	if len(lcb.Txs[0].Result.ContractResult.Result) == 0 {
		return fmt.Errorf("config block contract result should not empty")
	}
	var cc configPb.ChainConfig
	err = proto.Unmarshal(lcb.Txs[0].Result.ContractResult.Result, &cc)
	if err != nil {
		return err
	}

	mgr.hashType = cc.Crypto.Hash
	return nil
}

// Close archive mgr
// @Description:
// @receiver mgr
// @return error
func (mgr *ArchiveMgr) Close() error {
	if mgr.stopCh != nil {
		close(mgr.stopCh)
	}
	if mgr.fileRestore != nil {
		return mgr.fileRestore.Close()
	}
	return nil
}

// filterFileNames 扫描区块高度区间范围内所有的区块文件列表
// @Description:
// @param begin: block height
// @param end: block height
// @param path
// @return []*storePb.FileRange
// @return error
func filterFileNames(begin, end uint64, path string) ([]*storePb.FileRange, error) {
	heights, err := scanPath(path)
	if err != nil {
		return nil, err
	}
	rngs := bfdbFileRngs(heights)
	if len(rngs) == 0 {
		return nil, fmt.Errorf("filter archive fdb range should not empty")
	}

	var (
		si, ei int
	)
	for i, rng := range rngs {
		if begin == rng.Start {
			si = i
		}

		if rng.End == 0 {
			rng.End = end
		}
		if end == rng.End {
			ei = i
			break
		}
	}

	return rngs[si : ei+1], nil
}

// scanPath scan and return all xxx.fdb's start block height then sort it
// @Description:
// @param path
// @return []uint64
// @return error
func scanPath(path string) ([]uint64, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var heights []uint64
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}
		name := fi.Name()
		if !tbf.CheckSegmentName(name) {
			continue
		}
		if !strings.HasSuffix(name, tbf.DBFileSuffix) || len(name) != tbf.DBFileNameLen+len(tbf.DBFileSuffix) {
			continue
		}
		// index most time is the first height of bfdb rfile
		index, erri := strconv.ParseUint(name[:tbf.DBFileNameLen], 10, 64)
		if erri != nil || index == 0 {
			continue
		}
		heights = append(heights, index)
	}

	if len(heights) > 0 && err == nil {
		sort.Slice(heights, func(i, j int) bool {
			return heights[i] < heights[j]
		})
	}
	return heights, nil
}

// bfdbFileRngs calculate xxx.fdb contains block range, and last range.End = 0
// @Description:
// @param heights
// @return []*storePb.FileRange
func bfdbFileRngs(heights []uint64) []*storePb.FileRange {
	frs := make([]*storePb.FileRange, 0, len(heights)+1)
	if len(heights) == 0 {
		return frs
	}
	if len(heights) == 1 {
		frs = append(frs, &storePb.FileRange{
			FileName: tbf.Uint64ToSegmentName(heights[0]),
			Start:    heights[0] - 1,
			End:      0,
		})
		return frs
	}

	for i := 0; i < len(heights)-1; i++ {
		frs = append(frs, &storePb.FileRange{
			FileName: tbf.Uint64ToSegmentName(heights[i]),
			Start:    heights[i] - 1,
			End:      heights[i+1] - 2,
		})
	}

	lastI := len(heights) - 1
	frs = append(frs, &storePb.FileRange{
		FileName: tbf.Uint64ToSegmentName(heights[lastI]),
		Start:    heights[lastI] - 1,
		End:      0,
	})

	return frs
}
