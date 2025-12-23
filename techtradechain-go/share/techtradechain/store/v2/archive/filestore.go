/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0

*/

// Package archive implement
package archive

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"techtradechain.com/techtradechain/common/v2/json"
	lwsf "techtradechain.com/techtradechain/lws/file"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/store/v2/serialization"
	tbf "techtradechain.com/techtradechain/store/v2/types/blockfile"
	"techtradechain.com/techtradechain/store/v2/utils"
)

// Options for FileRestore
type Options struct {
	// NoSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	NoSync bool
	// SegmentSize of each Segment. This is just a target value, actual size
	// may differ. Default is 20 MB.
	SegmentSize int
	// SegmentCacheSize is the maximum number of segments that will be held in
	// memory for caching. Increasing this value may enhance performance for
	// concurrent read operations. Default is 1
	SegmentCacheSize int
}

// DefaultOptions for OpenRestore().
var DefaultOptions = &Options{
	NoSync:           false,    // Fsync after every write
	SegmentSize:      67108864, // 64 MB log Segment files.
	SegmentCacheSize: 25,       // Number of cached in-memory segments
}

// FileRestore manage xxx.fdb under bfdb/restoring dir
type FileRestore struct {
	mu          sync.RWMutex
	path        string
	opts        Options    // log options
	segments    []*Segment // all log segments
	currSegment *Segment   // current log Segment
	wbatch      Batch      // reusable write batch
	logger      protocol.Logger
}

//mergingSeg  *Segment   // merging Segment, write at least 2 segments' continuous to 1 bigger Segment

// Segment represents a single Segment rfile.
type Segment struct {
	name      string          // name of Segment rfile
	index     uint64          // first index of Segment
	lastIndex uint64          // last data index of Segment
	dFile     *tbf.FileWriter // data file handle
	iFile     *tbf.FileWriter // index file handle
	segIndexs []*tbf.SegIndex
}

// OpenRestore open a restore FileRestore
//
//	@Description:
//	@param path
//	@param opts
//	@param logger
//	@return *FileRestore
//	@return error
func OpenRestore(path string, opts *Options, logger protocol.Logger) (*FileRestore, error) {
	if opts == nil {
		opts = DefaultOptions
	}
	if opts.SegmentCacheSize <= 0 {
		opts.SegmentCacheSize = DefaultOptions.SegmentCacheSize
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultOptions.SegmentSize
	}

	var err error
	if path, err = filepath.Abs(path); err != nil {
		return nil, err
	}

	l := &FileRestore{
		path:   path,
		opts:   *opts,
		logger: logger,
	}
	if err = os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}
	if err = utils.CheckPathRWMod(path); err != nil {
		return nil, err
	}
	if err = l.load(); err != nil {
		return nil, err
	}

	return l, nil
}

// load load Segment files from path
// @Description: 从指定目录加载segment文件
// @receiver l
// @param path
// @return error
func (l *FileRestore) load() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.resetMergeSegmentEnv(); err != nil {
		return err
	}

	fis, err := ioutil.ReadDir(l.path)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		name := fi.Name()
		if !tbf.CheckSegmentName(name) {
			continue
		}

		// reset merge Segment uncompleted scenario
		// move xxx.bak to xxx and remove xxx.merging file while load
		if strings.HasSuffix(name, tbf.BakFileSuffix) {
			// move Segment file xxx.fdb.bak to xxx.fdb
			newName := strings.ReplaceAll(name, tbf.BakFileSuffix, "")
			newPath := path.Join(l.path, newName)
			if exist, _ := utils.PathExists(newPath); exist {
				if err = os.RemoveAll(newPath); err != nil {
					l.logger.Warnf("remove %s failed: %v", newPath, err)
				}
			}
			if err = os.Rename(path.Join(l.path, name), newPath); err != nil {
				return err
			}
			name = newName
		}

		// read xxx.restoring.json
		if !strings.HasSuffix(name, tbf.RestoringIndexFileName) ||
			len(name) != tbf.DBFileNameLen+len(tbf.RestoringIndexFileName) {
			continue
		}

		segName := name[:tbf.DBFileNameLen]

		// if xxx.fdb missed, then remove index file
		//exist, _ := utils.PathExists(path.Join(l.path, fmt.Sprintf("%s%s", segName, tbf.DBFileSuffix)))
		//if !exist {
		//	if err = os.RemoveAll(path.Join(l.path, name)); err != nil {
		//		return err
		//	}
		//	continue
		//}

		if err = l.loadIndexFile(segName, name); err != nil {
			return err
		}
	}

	l.sortSegs()

	return nil
}

// loadIndexFile handle index file data
// @Description:
// @receiver l
// @param segName
// @param name
// @return error
func (l *FileRestore) loadIndexFile(segName string, name string) error {
	start, err1 := tbf.SegmentNameToUint64(segName)
	if err1 != nil {
		return fmt.Errorf("file name: %s invalidate, err: %v", name, err1)
	}

	if start == 0 {
		return fmt.Errorf("file name: %s invalidate", name)
	}

	seg, errs := l.newSegment(segName, start, start)
	if errs != nil {
		return errs
	}

	seg.segIndexs, errs = tbf.LoadArchiveSegIndex(seg.iFile)
	if errs != nil {
		return errs
	}

	if len(seg.segIndexs) > 0 {
		seg.lastIndex = seg.index + uint64(len(seg.segIndexs)) - 1
	}

	l.segments = append(l.segments, seg)
	return nil
}

// removeNilSegments remove nil l.segments
// @Description:
// @receiver l
func (l *FileRestore) removeNilSegments() {
	i, j := 0, 0
	for j < len(l.segments) {
		if l.segments[j] != nil {
			l.segments[i] = l.segments[j]
			i++
		}
		j++
	}
	l.segments = l.segments[:i]
}

// sortSegs make l.segments in increase order
// @Description:
// @receiver l
func (l *FileRestore) sortSegs() {
	sort.Slice(l.segments, func(i, j int) bool {
		return l.segments[i].lastIndex < l.segments[j].lastIndex
	})
}

// appendIndexEntry add next time
// @Description:
// @receiver l
// @param s
// @return error
func (l *FileRestore) appendIndexEntry(s *Segment, index *tbf.SegIndex) error {
	data, err := json.Marshal(index)
	if err != nil {
		return err
	}
	return tbf.AppendFileEntry(data, s.iFile, len(data), l.logger)
}

// Close the log.
//
//	@Description:
//	@receiver l
//	@return error
func (l *FileRestore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var err error
	for _, seg := range l.segments {
		if err = l.closeSegmentFile(seg); err != nil {
			l.logger.Errorf("close seg: %s failed: %v", seg.name, err)
		}
	}

	return err
}

// Write
// @Description: Write an entry to the block rfile db.
// @receiver l
// @param index
// @param data
// @return fileName
// @return offset
// @return blkLen
// @return err
func (l *FileRestore) Write(index uint64, data []byte,
	indexMeta *serialization.BlockIndexMeta) (*serialization.BlockIndexMeta, error) {

	l.wbatch.Write(index, data, indexMeta)
	bwi, err := l.writeBatch(&l.wbatch)
	if err != nil {
		return nil, err
	}
	return bwi, nil
}

// GetSegsInfo get l.segments info
// @Description:
// @receiver l
// @return []string
func (l *FileRestore) GetSegsInfo() []*storePb.FileRange {
	fileRanges := make([]*storePb.FileRange, 0, len(l.segments))
	for _, seg := range l.segments {
		if seg == nil {
			continue
		}
		fileRanges = append(fileRanges, &storePb.FileRange{
			FileName: seg.name,
			Start:    seg.index - 1,
			End:      seg.lastIndex - 1,
		})
	}
	return fileRanges
}

// cycle
// @Description: Cycle the old Segment for a new Segment.
// @receiver l
// @return error
func (l *FileRestore) cycle() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var err error
	if l.opts.NoSync {
		if err = l.syncSegment(l.currSegment); err != nil {
			return err
		}
	}
	if err = l.currSegment.dFile.Lfile.Wfile.Close(); err != nil {
		return err
	}

	// remove pre last Segment name's end suffix
	orgPath := l.currSegment.dFile.Path
	if !strings.HasSuffix(orgPath, tbf.DBFileSuffix) {
		return fmt.Errorf("Segment rfile dot not end with %s", tbf.DBFileSuffix)
	}
	segName := tbf.Uint64ToSegmentName(l.currSegment.lastIndex + 1)
	l.currSegment.dFile.Path = tbf.SegmentPath(segName, l.path)
	nSeg, errn := l.newSegment(segName, l.currSegment.lastIndex+1, l.currSegment.lastIndex+1)
	if errn != nil {
		return errn
	}
	l.segments = append(l.segments, nSeg)
	l.currSegment = nSeg
	return nil
}

// Batch of entries. Used to write multiple entries at once using WriteBatch().
//
//	@Description:
type Batch struct {
	entry batchEntry
	data  []byte
}

// batchEntry
// @Description:
type batchEntry struct {
	index      uint64
	size       int
	entryIndex *serialization.BlockIndexMeta
}

// Write
// @Description: Write an entry to the batch
// @receiver b
// @param index
// @param data
func (b *Batch) Write(index uint64, data []byte, indexMeta *serialization.BlockIndexMeta) {
	b.entry = batchEntry{index, len(data), indexMeta}
	b.data = data
}

//// WriteBatch writes the entries in the batch to the log in the order that they
//// were added to the batch. The batch is cleared upon a successful return.
////  @Description:
////  @receiver l
////  @param b
////  @return *storePb.StoreInfo
////  @return error
//func (l *FileRestore) WriteBatch(b *Batch) (*serialization.BlockIndexMeta, error) {
//	l.mu.Lock()
//	defer l.mu.Unlock()
//
//	if b.entry.size == 0 {
//		return nil, nil
//	}
//	return l.writeBatch(b)
//}

// writeBatch 写一个batch 返回索引信息
// @Description:
// @receiver l
// @param b
// @return *storePb.StoreInfo
// @return error
func (l *FileRestore) writeBatch(b *Batch) (*serialization.BlockIndexMeta, error) {
	if err := l.updateCurrSegment(b); err != nil {
		return nil, err
	}

	s := l.currSegment
	// check cycle
	if len(s.dFile.Ebuf) > l.opts.SegmentSize {
		// tail Segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return nil, err
		}
		s = l.currSegment
	}

	if err := tbf.AppendFileEntry(b.data, s.dFile, b.entry.size, l.logger); err != nil {
		return nil, err
	}

	s.lastIndex = b.entry.index
	epos := s.dFile.Epos[len(s.dFile.Epos)-1]
	b.entry.entryIndex.Index = &storePb.StoreInfo{
		FileName: l.currSegment.name[:tbf.DBFileNameLen],
		Offset:   uint64(epos.Pos + epos.PrefixLen),
		ByteLen:  uint64(b.entry.size),
	}
	newIndex := &tbf.SegIndex{
		Height: s.lastIndex - 1,
		BIndex: b.entry.entryIndex,
	}
	if err := l.appendIndexEntry(l.currSegment, newIndex); err != nil {
		return nil, err
	}
	s.segIndexs = append(s.segIndexs, newIndex)

	if !l.opts.NoSync {
		// power down below may cause file name update uncompleted,
		// but will be correction in next open and scan time
		if err1 := l.syncSegment(s); err1 != nil {
			return nil, err1
		}
	}

	return b.entry.entryIndex, nil
}

// updateCurrSegment add next time
// @Description:
// @receiver l
// @param b
// @return error
func (l *FileRestore) updateCurrSegment(b *Batch) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// find belongs .fdb
	found := false
	if l.currSegment != nil {
		if b.entry.index == l.currSegment.lastIndex+uint64(1) {
			found = true
		} else {
			for _, seg := range l.segments {
				if b.entry.index == seg.lastIndex+uint64(1) {
					l.currSegment = seg
					found = true
					break
				}
			}
		}
	}

	// can not find, create new Segment
	var err error
	if !found {
		// clean old Segment memory data to save memory resource
		if l.currSegment != nil {
			l.currSegment.dFile.Epos = make([]tbf.Bpos, 0)
			l.currSegment.dFile.Ebuf = []byte{}
		}

		segName := tbf.Uint64ToSegmentName(b.entry.index)
		nSeg, errn := l.newSegment(segName, b.entry.index, b.entry.index)
		if errn != nil {
			return errn
		}
		l.segments = append(l.segments, nSeg)
		l.currSegment = nSeg
	}

	// load this .fdb's content
	s := l.currSegment
	if len(s.dFile.Epos) == 0 {
		s.dFile.Ebuf, s.dFile.Epos, err = tbf.LoadEntriesForRestarting(s.dFile.Path)
		if err != nil {
			return err
		}
		s.lastIndex = s.lastIndex + uint64(len(s.dFile.Epos))
		// correct index file data from .fdb
		if len(s.segIndexs) != len(s.dFile.Epos) {

			mapHeight := make(map[uint64]bool)
			for j := 0; j < len(s.segIndexs); j++ {
				segIndex := s.segIndexs[j]
				mapHeight[segIndex.Height] = true
			}

			// if the height is not exist , then append new segIndex to segIndexs
			for height := s.index; height < s.lastIndex; height++ {
				//if s.segIndexs[height] == nil {
				if exist := mapHeight[height]; !exist {
					b.entry.entryIndex.Index = &storePb.StoreInfo{
						StoreType: storePb.DataStoreType_FILE_STORE,
						FileName:  s.name,
						Offset:    uint64(s.dFile.Epos[height].Pos),
						ByteLen:   uint64(s.dFile.Epos[height].End - s.dFile.Epos[height].PrefixLen),
					}
					newIndex := &tbf.SegIndex{
						Height: height - 1,
						BIndex: b.entry.entryIndex,
					}
					if errs := l.appendIndexEntry(s, newIndex); errs != nil {
						return errs
					}
					s.segIndexs = append(s.segIndexs, newIndex)
				}
			}
		}
	}

	return nil
}

// Sync performs a fsync on the log. This is not necessary when the
// NoSync option is set to false.
// @Description:
// @receiver l
// @return error
func (l *FileRestore) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.currSegment.dFile.Lfile.Wfile.Sync(); err != nil {
		return err
	}

	return l.currSegment.iFile.Lfile.Wfile.Sync()
}

// newSegment new and init Segment
// @Description:
// @receiver l
// @param segName
// @param index
// @param lastIndex
// @return *Segment
func (l *FileRestore) newSegment(segName string, index, lastIndex uint64) (*Segment, error) {
	seg := &Segment{
		name:      segName,
		index:     index,
		lastIndex: lastIndex,
		segIndexs: make([]*tbf.SegIndex, 0),
	}

	var err error
	seg.dFile, err = tbf.NewFileWriter(tbf.SegmentPath(segName, l.path))
	if err != nil {
		return nil, err
	}

	seg.iFile, err = tbf.NewFileWriter(tbf.SegmentIndexPath(segName, l.path))
	return seg, err
}

// LastSegment return last Segment in l.segments
// @Description:
// @receiver l
// @return *Segment
func (l *FileRestore) LastSegment() *Segment {
	if len(l.segments) == 0 {
		return nil
	}
	l.sortSegs()
	return l.segments[len(l.segments)-1]
}

// newMergeSegment new and init merge Segment
// @Description:
// @receiver l
// @param segName
// @param index
// @param lastIndex
// @return *Segment
func (l *FileRestore) newMergeSegment(segName string, index uint64) (*Segment, error) {
	seg := &Segment{
		name:      segName,
		index:     index,
		lastIndex: index,
		segIndexs: make([]*tbf.SegIndex, 0),
	}

	var err error
	seg.dFile, err = tbf.NewFileWriter(
		fmt.Sprintf("%s%s", tbf.SegmentPath(segName, l.path), tbf.MergingFileSuffix))
	if err != nil {
		return nil, err
	}

	seg.iFile, err = tbf.NewFileWriter(
		fmt.Sprintf("%s%s", tbf.SegmentIndexPath(segName, l.path), tbf.MergingFileSuffix))
	return seg, err
}

// resetMergeSegmentEnv reset merge Segment env handle .merging file and .bak file
// @Description:
// @receiver l
// @return error
func (l *FileRestore) resetMergeSegmentEnv() error {
	rPath := ""
	normalSNs, bakSNs, mergingSNs, err := l.filterMergingFile()
	if err != nil {
		return err
	}

	if err = l.updateSegments(normalSNs); err != nil {
		return err
	}

	// merging dir is normal
	if len(bakSNs) == 0 && len(mergingSNs) == 0 {
		return nil
	}

	// .bak file not remove finished and after merged, remove .bak file
	if len(bakSNs) > 0 && len(mergingSNs) == 0 {
		if err = l.removeFiles(bakSNs); err != nil {
			return err
		}
	}

	// only .merging file exists, merge stop in merging start time, remove .merge file
	if len(bakSNs) == 0 && len(mergingSNs) > 0 {
		for _, msn := range mergingSNs {
			rPath = path.Join(l.path, msn)
			if err = os.RemoveAll(rPath); err != nil {
				return fmt.Errorf("remove merge path: %s failed: %v", rPath, err)
			}
			l.logger.Warnf("removed useless restore merging data: %s", rPath)
		}
	}

	return l.mergingFileLeft(mergingSNs, bakSNs)
}

// updateSegments update l.segments according to normalSNs
// @Description:
// @receiver l
// @param normalSNs
// @return error
func (l *FileRestore) updateSegments(normalSNs map[string][]string) error {
	var (
		err   error
		rPath string
	)

	// remove useless path
	uks := make([]string, 0)
	for k, v := range normalSNs {
		if len(v) == 1 {
			if strings.HasSuffix(v[0], tbf.DBFileSuffix) { // only .fdb file exists
				rPath = path.Join(l.path, v[0])
				if err = os.RemoveAll(rPath); err != nil {
					return fmt.Errorf("remove useless path: %s failed: %v", rPath, err)
				}

				l.delSegmentByName(k)
				uks = append(uks, k)
				l.logger.Warnf("removed broken restore data: %s", rPath)
			} else { // only .restoring.json file exists
				bfdbDir := strings.Replace(l.path, tbf.RestoringPath, "", 1)
				bfdbPath := tbf.SegmentPath(k, bfdbDir)
				fdbPath := tbf.SegmentPath(k, l.path)
				indexPath := tbf.SegmentIndexPath(k, l.path)
				if exists, _ := utils.PathExists(bfdbPath); !exists { // .fdb file not exists in bfdbDir
					if err = os.RemoveAll(indexPath); err != nil {
						return fmt.Errorf("remove useless path: %s failed: %v", indexPath, err)
					}
					l.logger.Warnf("index: %s exists, but fdb:%s is not exists, "+
						"will abandon this index file, please restore this part again", indexPath, fdbPath)
					l.delSegmentByName(k)
					uks = append(uks, k)
				} else { // .fdb file exists in bfdbDir
					if err = tbf.CopyThenRemoveFile(bfdbPath, fdbPath, nil); err != nil {
						l.logger.Warnf("copy %s then remove %s failed: %v", bfdbPath, fdbPath, err)
						return err
					}
				}
			}
		}
	}

	for _, uk := range uks {
		delete(normalSNs, uk)
	}

	if len(normalSNs) != len(l.segments) {
		for i := 0; i < len(l.segments); i++ {
			if normalSNs[l.segments[i].name] == nil {
				l.segments[i] = nil
			}
		}
		l.removeNilSegments()
	}
	l.logger.Infof("update segments dir file cnt: %d, l.segments cnt: %d",
		len(normalSNs), len(l.segments))
	return err
}

// mergingFileLeft handle leave .merging file in bfdb/restoring
// .bak and .merging file both exists:
//  1. xxx.fdb.bak、xxx.restoring.bak、xxx.fdb.merging and xxx.restoring.merging all exists,
//     rename to xxx.fdb and xxx.restoring, and remove all .merge file
//  2. only one of .bak pair exists
//     2.1. .merge file pair both exists: merge stop in merging time,
//     rename .bak to xxx.fdb and xxx.restoring than remove all .merge files
//     2.2. only one of .merge file pair is exists: merge stop in mergingSegment save time,
//     we can not make sure data is still correct, so will remove the .bak whose name equal .merging file
//     and other .bak execute rename action then remove all .merging file,
//     user need resend this block data to node again
//
// @Description:
// @receiver l
// @param mergingSNs
// @param bakSNs
// @return error
func (l *FileRestore) mergingFileLeft(mergingSNs []string, bakSNs map[string][]string) error {
	var (
		err          error
		rPath, nPath string
	)
	if len(mergingSNs) == 2 {
		for _, msn := range mergingSNs {
			rPath = path.Join(l.path, msn)
			if err = os.RemoveAll(rPath); err != nil {
				return fmt.Errorf("remove merge path: %s failed: %v", rPath, err)
			}
			l.logger.Warnf("removed useless restore merging data: %s", rPath)
		}
		if err = l.renameFiles(bakSNs); err != nil {
			return err
		}
	}

	bothExists := true
	for _, bakM := range bakSNs {
		if len(bakM) < 2 {
			bothExists = false
		}
	}
	if bothExists && len(mergingSNs) == 1 {
		mName := mergingSNs[0][:tbf.DBFileNameLen]

		// remove .merging files
		rPath = path.Join(l.path, mergingSNs[0])
		if err = os.RemoveAll(rPath); err != nil {
			return fmt.Errorf("remove merge path: %s failed: %v", rPath, err)
		}
		l.logger.Infof("removed useless restore merging data: %s", rPath)
		rPath = tbf.SegmentPath(mName, l.path)
		if exi, _ := utils.PathExists(rPath); exi {
			if err = os.RemoveAll(rPath); err != nil {
				return fmt.Errorf("remove merge fdb path: %s failed: %v", rPath, err)
			}
			l.logger.Warnf("removed useless restore merging data: %s", rPath)
		}
		delete(bakSNs, mName)

		// rename other .bak
		for _, bakM := range bakSNs {
			if len(bakM) == 1 {
				rPath = path.Join(l.path, bakM[0])
				if err = os.RemoveAll(rPath); err != nil {
					return fmt.Errorf("remove merge path: %s failed: %v", rPath, err)
				}
				l.logger.Warnf("removed useless restore bak data: %s", rPath)
				rPath = tbf.SegmentPath(bakM[0][:tbf.DBFileNameLen], l.path)
				if exi, _ := utils.PathExists(rPath); exi {
					if err = os.RemoveAll(rPath); err != nil {
						return fmt.Errorf("remove fdb path: %s failed: %v", rPath, err)
					}
					l.logger.Warnf("removed useless restore bak data: %s", rPath)
				}
			} else {
				for _, bak := range bakM {
					rPath = path.Join(l.path, bak)
					nPath = path.Join(l.path, strings.ReplaceAll(bak, tbf.BakFileSuffix, ""))
					if err = os.Rename(rPath, nPath); err != nil {
						return fmt.Errorf("rename bak path: %s to new path:%s failed: %v", rPath, nPath, err)
					}
					l.logger.Infof("renamed from %s to %s", rPath, nPath)
				}
			}
		}
	}
	return nil
}

// filterMergingFile filter merging uncompleted file in bfdb/restoring
// @Description:
// @receiver l
// @return map[string][]string
// @return []string
// @return error
func (l *FileRestore) filterMergingFile() (map[string][]string, map[string][]string, []string, error) {
	fis, err := ioutil.ReadDir(l.path)
	if err != nil {
		return nil, nil, nil, err
	}

	normalSNs := make(map[string][]string)
	bakSNs := make(map[string][]string)
	mergingSNs := make([]string, 0)
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		name := fi.Name()
		if !tbf.CheckSegmentName(name) {
			continue
		}

		if strings.HasSuffix(name, tbf.BakFileSuffix) {
			na := name[:tbf.DBFileNameLen]
			if bakSNs[na] == nil {
				bakSNs[na] = []string{name}
			} else {
				bakSNs[na] = append(bakSNs[na], name)
			}
			continue
		}

		if strings.HasSuffix(name, tbf.DBFileSuffix) || strings.HasSuffix(name, tbf.RestoringIndexFileName) {
			na := name[:tbf.DBFileNameLen]
			if normalSNs[na] == nil {
				normalSNs[na] = []string{name}
			} else {
				normalSNs[na] = append(normalSNs[na], name)
			}
			continue
		}

		if strings.HasSuffix(name, tbf.MergingFileSuffix) {
			mergingSNs = append(mergingSNs, name)
		}
	}

	return normalSNs, bakSNs, mergingSNs, err
}

// removeFiles remove .bak Segment file
// @Description:
// @receiver l
// @param bakSNs
// @return error
func (l *FileRestore) removeFiles(bakSNs map[string][]string) error {
	var (
		err   error
		rPath string
	)
	for _, bakM := range bakSNs {
		for _, bak := range bakM {
			rPath = path.Join(l.path, bak)
			if err = os.RemoveAll(rPath); err != nil {
				return fmt.Errorf("remove bak path: %s failed: %v", rPath, err)
			}
			l.logger.Warnf("removed useless bak data: %s", rPath)
		}
	}
	return nil
}

// removeFiles rename .bak Segment file
// @Description:
// @receiver l
// @param bakSNs
// @return error
func (l *FileRestore) renameFiles(bakSNs map[string][]string) error {
	var (
		err          error
		rPath, nPath string
	)
	for _, bakM := range bakSNs {
		for _, bak := range bakM {
			rPath = path.Join(l.path, bak)
			nPath = path.Join(l.path, strings.ReplaceAll(bak, tbf.BakFileSuffix, ""))
			if err = os.Rename(rPath, nPath); err != nil {
				return fmt.Errorf("rename bak path: %s to new path:%s failed: %v", rPath, nPath, err)
			}
			l.logger.Infof("renamed from %s to %s", rPath, nPath)
		}
	}
	return nil
}

// mergeSegment merge last small segments to big one
// @Description:
// @receiver mgr
// @return error
func (l *FileRestore) mergeSegment() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.removeNilSegments()

	if err := l.resetMergeSegmentEnv(); err != nil {
		return fmt.Errorf("err 1: %v", err)
	}
	if len(l.segments) < 2 {
		return nil
	}

	l.sortSegs()
	// find out continuous Segment
	csi := l.findContSegs()
	// can not find continuous Segment
	if len(csi) < 2 {
		return nil
	}

	var (
		err  error
		mSeg *Segment
	)
	csii := len(csi) - 1
	bakSNs := make([]string, 0)
	for i := len(csi) - 1; i >= 0; i-- {
		if mSeg != nil && mSeg.dFile.Lfile.Wfile.Size() >= int64(l.opts.SegmentSize) {
			if _, err = l.saveMergeSegment(mSeg, csi[csii]); err != nil {
				return err
			}
			i = i + 1
			mSeg = nil
			continue
		}

		if l.segments[csi[i]] == nil {
			continue
		}
		cSeg := l.segments[csi[i]]
		if err = l.openSegFile(cSeg); err != nil {
			return err
		}

		// skip single xxx.fdb file size >= l.opts.SegmentSize
		if cSeg.dFile.Lfile.Wfile.Size() >= int64(l.opts.SegmentSize) {
			cSeg = nil
			continue
		}

		if mSeg == nil {
			if mSeg, err = l.newMergeSegment(cSeg.name, cSeg.index); err != nil {
				return err
			}
			csii = i
		}

		bakSNs, err = l.doMergeSegment(mSeg, cSeg, csi[i], bakSNs)
		if err != nil {
			return err
		}
		cSeg = nil
	}

	if _, err = l.saveMergeSegment(mSeg, csi[csii]); err != nil {
		return err
	}
	if err = l.removeBakSeg(bakSNs); err != nil {
		return err
	}
	l.removeNilSegments()
	return nil
}

// findContSegs find out continuous segments
// @Description:
// @receiver l
// @return []int
func (l *FileRestore) findContSegs() []int {
	cnt := len(l.segments)
	csi := []int{cnt - 1}
	for i := cnt - 1; i > 0; i-- {
		if l.segments[i].index == l.segments[i-1].lastIndex+1 {
			csi = append(csi, i-1)
		} else {
			break
		}
	}
	return csi
}

// removeBakSeg add next time
// @Description:
// @receiver l
// @param bakSegNames
// @return error
func (l *FileRestore) removeBakSeg(bakSN []string) error {
	var err error
	for _, bsn := range bakSN {
		bakPath := fmt.Sprintf("%s%s", tbf.SegmentPath(bsn, l.path), tbf.BakFileSuffix)
		if err = os.RemoveAll(bakPath); err != nil {
			l.logger.Errorf("remove bak fdb file: %s failed: %v", bakPath, err)
		}
		bakPath = fmt.Sprintf("%s%s", tbf.SegmentIndexPath(bsn, l.path), tbf.BakFileSuffix)
		if err = os.RemoveAll(bakPath); err != nil {
			l.logger.Errorf("remove bak index file: %s failed: %v", bakPath, err)
		}
	}

	return nil
}

// doMergeSegment merge cSeg to mSeg
// @Description:
// @receiver l
// @param seg
// @param replacePos
// @return error
func (l *FileRestore) doMergeSegment(mSeg, cSeg *Segment, replacePos int, bakSNs []string) ([]string, error) {
	if err := l.closeSegmentFile(cSeg); err != nil {
		return bakSNs, err
	}
	// write xxx.fdb to xxx.fdb.merging
	data, err := os.ReadFile(tbf.SegmentPath(cSeg.name, l.path))
	if err != nil {
		return bakSNs, err
	}

	pos := mSeg.dFile.Lfile.Wfile.Size()
	dLen, err2 := mSeg.dFile.Lfile.Wfile.WriteAt(data, pos)
	if err2 != nil {
		return bakSNs, err
	}
	if dLen != len(data) {
		return bakSNs, fmt.Errorf("append cSeg: %s data to mergingSeg: %s uncompleted", cSeg.name, mSeg.name)
	}

	// write xxx.restoring.json to xxx.fdb.restoring.json.merging
	for _, si := range cSeg.segIndexs {
		if err = l.appendMergeSegmentIndex(mSeg, si, uint64(pos), mSeg.name); err != nil {
			return bakSNs, err
		}
	}

	// move Segment file xxx.fdb to xxx.fdb.bak
	if err = os.Rename(tbf.SegmentPath(cSeg.name, l.path),
		fmt.Sprintf("%s%s", tbf.SegmentPath(cSeg.name, l.path), tbf.BakFileSuffix)); err != nil {
		return bakSNs, err
	}
	// move Segment file xxx.restoring.json to xxx.restoring.json.bak
	if err = os.Rename(tbf.SegmentIndexPath(cSeg.name, l.path),
		fmt.Sprintf("%s%s", tbf.SegmentIndexPath(cSeg.name, l.path), tbf.BakFileSuffix)); err != nil {
		return bakSNs, err
	}
	l.segments[replacePos] = nil
	if bakSNs == nil {
		bakSNs = make([]string, 0)
	}
	bakSNs = append(bakSNs, cSeg.name)
	return bakSNs, nil
}

// appendMergeSegmentIndex add next time
// @Description:
// @receiver l
// @param si
// @param preOffset
// @return error
func (l *FileRestore) appendMergeSegmentIndex(mSeg *Segment, si *tbf.SegIndex, preOffset uint64, fn string) error {
	nsi := &tbf.SegIndex{
		Height: si.Height,
		BIndex: &serialization.BlockIndexMeta{
			Height:         si.BIndex.Height,
			BlockTimestamp: si.BIndex.BlockTimestamp,
			BlockHash:      si.BIndex.BlockHash,
			TxIds:          si.BIndex.TxIds,
			RwSets:         si.BIndex.RwSets,
		},
	}
	nsi.BIndex.Index = &storePb.StoreInfo{
		StoreType: si.BIndex.Index.StoreType,
		FileName:  fn,
		Offset:    si.BIndex.Index.Offset + preOffset,
		ByteLen:   si.BIndex.Index.ByteLen,
	}
	nsi.BIndex.MetaIndex = &storePb.StoreInfo{
		StoreType: si.BIndex.MetaIndex.StoreType,
		FileName:  fn,
		Offset:    si.BIndex.MetaIndex.Offset,
		ByteLen:   si.BIndex.MetaIndex.ByteLen,
	}

	nsi.BIndex.TxsIndex = make([]*storePb.StoreInfo, 0, len(si.BIndex.TxsIndex))
	for _, item := range si.BIndex.TxsIndex {
		nsi.BIndex.TxsIndex = append(nsi.BIndex.TxsIndex, &storePb.StoreInfo{
			StoreType: item.StoreType,
			FileName:  fn,
			Offset:    item.Offset,
			ByteLen:   item.ByteLen,
		})
	}

	nsi.BIndex.RWSetsIndex = make([]*storePb.StoreInfo, 0, len(si.BIndex.RWSetsIndex))
	for _, item := range si.BIndex.RWSetsIndex {
		nsi.BIndex.RWSetsIndex = append(nsi.BIndex.RWSetsIndex, &storePb.StoreInfo{
			StoreType: item.StoreType,
			FileName:  fn,
			Offset:    item.Offset,
			ByteLen:   item.ByteLen,
		})
	}

	if err := l.appendIndexEntry(mSeg, nsi); err != nil {
		return err
	}
	mSeg.segIndexs = append(mSeg.segIndexs, nsi)
	mSeg.lastIndex = si.BIndex.Height + 1
	return nil
}

// openSegFile add next time
// @Description:
// @receiver l
// @param mSeg
// @return error
func (l *FileRestore) openSegFile(cSeg *Segment) error {
	if cSeg == nil {
		return nil
	}

	// file closed, reopen it
	var err error
	if cSeg.dFile.Lfile.Wfile.Size() == -1 {
		cSeg.dFile.Lfile.Wfile, err = lwsf.NewFile(cSeg.dFile.Path)
		if err != nil {
			return err
		}
	}

	if cSeg.iFile.Lfile.Wfile.Size() == -1 {
		cSeg.iFile.Lfile.Wfile, err = lwsf.NewFile(cSeg.iFile.Path)
		if err != nil {
			return err
		}
	}

	return nil
}

// saveMergeSegment save merging Segment to l.segments then set l.mergingSeg to nil
// @Description:
// @receiver l
// @param replacePos
// @return error
func (l *FileRestore) saveMergeSegment(mSeg *Segment, replacePos int) (*Segment, error) {
	if mSeg == nil {
		return nil, nil
	}
	err := l.closeSegmentFile(mSeg)
	if err != nil { //&& !strings.Contains(err.Error(), "file already closed") {
		return nil, err
	}

	// move merging Segment file xxx.fdb.merging to xxx.fdb
	oPath := fmt.Sprintf("%s%s", tbf.SegmentPath(mSeg.name, l.path), tbf.MergingFileSuffix)
	nPath := tbf.SegmentPath(mSeg.name, l.path)
	l.logger.Debugf(" saveMergeSegment rename %s to %s", oPath, nPath)
	if err = os.Rename(oPath, nPath); err != nil {
		return nil, err
	}

	// move merging Segment file xxx.restoring.json.merging to xxx.restoring.json
	oPath = fmt.Sprintf("%s%s", tbf.SegmentIndexPath(mSeg.name, l.path), tbf.MergingFileSuffix)
	nPath = tbf.SegmentIndexPath(mSeg.name, l.path)
	l.logger.Debugf("saveMergeSegment rename %s to %s", oPath, nPath)
	if err = os.Rename(oPath, nPath); err != nil {
		return nil, err
	}

	if l.segments[replacePos], err = l.newSegment(mSeg.name, mSeg.index, mSeg.lastIndex); err != nil {
		return nil, err
	}

	l.segments[replacePos].segIndexs = mSeg.segIndexs
	return mSeg, nil
}

// syncSegment sync Segment file
// @Description:
// @receiver l
// @param seg
// @return error
func (l *FileRestore) syncSegment(seg *Segment) error {
	if err := seg.dFile.Lfile.Wfile.Sync(); err != nil {
		return err
	}
	return seg.iFile.Lfile.Wfile.Sync()
}

// closeSegmentFile close Segment file
// @Description:
// @receiver l
// @param seg
// @return error
func (l *FileRestore) closeSegmentFile(seg *Segment) (err error) {
	defer func() {
		if err != nil && strings.Contains(err.Error(), "file already closed") {
			err = nil
		}
	}()

	if seg == nil {
		return nil
	}
	err = l.syncSegment(seg)
	if err != nil {
		return err
	}

	if err = seg.dFile.Lfile.Wfile.Close(); err != nil {
		return err
	}
	return seg.iFile.Lfile.Wfile.Close()
}

// closeSegmentByName add next time
// @Description:
// @receiver l
// @param seg
// @return error
func (l *FileRestore) closeSegmentByName(segName string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var seg *Segment
	for i := 0; i < len(l.segments); i++ {
		if l.segments[i].name == segName {
			seg = l.segments[i]
		}
	}

	return l.closeSegmentFile(seg)
}

// delSegmentByName del restored Segment data by segName
// @Description:
// @receiver l
// @param seg
// @return error
func (l *FileRestore) delSegmentByName(segName string) {
	if len(l.segments) == 0 {
		return
	}
	for i := 0; i < len(l.segments); i++ {
		if l.segments[i].name == segName {
			if i == len(l.segments)-1 {
				l.segments = l.segments[:len(l.segments)-1]
				l.logger.Infof("del Segment: %s, left: %d", segName, len(l.segments))
				return
			}
			l.segments[i] = l.segments[i+1]
		}
	}
	l.segments = l.segments[:len(l.segments)-1]
	l.logger.Infof("del Segment: %s, left: %d", segName, len(l.segments))
}

// ToString Segment to string
// @Description:
// @receiver l
// @param seg
// @return string
func (l *FileRestore) ToString(seg *Segment) string {
	if seg == nil {
		return "seg is nil"
	}
	return fmt.Sprintf("name: %s, index: %d, lastIndex: %d, segIndexs count: %d, dFile: %s, iFile: %s",
		seg.name, seg.index, seg.lastIndex, len(seg.segIndexs), seg.dFile.Path, seg.iFile.Path)
}
