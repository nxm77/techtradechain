// Package blockfiledb package
package blockfiledb

/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0

*/
import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	tbf "techtradechain.com/techtradechain/store/v2/types/blockfile"
	"techtradechain.com/techtradechain/store/v2/utils"
	"github.com/tidwall/tinylru"
)

var (
	// ErrCorrupt is returns when the log is corrupt.
	ErrCorrupt = errors.New("log corrupt")

	// ErrClosed is returned when an operation cannot be completed because
	// the log is closed.
	ErrClosed = errors.New("log closed")

	// ErrNotFound is returned when an entry is not found.
	ErrNotFound = errors.New("not found")

	// ErrOutOfOrder is returned from Write() when the index is not equal to
	// LastIndex()+1. It's required that log monotonically grows by one and has
	// no gaps. Thus, the series 10,11,12,13,14 is valid, but 10,11,13,14 is
	// not because there's a gap between 11 and 13. Also, 10,12,11,13 is not
	// valid because 12 and 11 are out of order.
	ErrOutOfOrder = errors.New("out of order")

	// ErrInvalidateIndex wrap "invalidate rfile index"
	ErrInvalidateIndex = errors.New("invalidate rfile index")

	// ErrBlockWrite wrap "write block wfile size invalidate"
	ErrBlockWrite = errors.New("write block wfile size invalidate")
)

const (
	rFileMissedMessageTemplate = "bfdb rfile:%s missed"
)

// Options for BlockFile
type Options struct {
	// NoSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	NoSync bool
	// SegmentSize of each segment. This is just a target value, actual size
	// may differ. Default is 64 MB.
	SegmentSize int
	// SegmentCacheSize is the maximum number of segments that will be held in
	// memory for caching. Increasing this value may enhance performance for
	// concurrent read operations. Default is 1
	SegmentCacheSize int
	// NoCopy allows for the Read() operation to return the raw underlying data
	// slice. This is an optimization to help minimize allocations. When this
	// option is set, do not modify the returned data because it may affect
	// other Read calls. Default false
	NoCopy bool
	// UseMmap It is a method of memory-mapped rfile I/O. It implements demand
	// paging because rfile contents are not read from disk directly and initially
	// do not use physical RAM at all
	UseMmap bool

	// ReadTimeOut read block file time out conf (ms)
	ReadTimeOut int64
}

// DefaultOptions for Open().
var DefaultOptions = &Options{
	NoSync:           false,    // Fsync after every write
	SegmentSize:      67108864, // 64 MB log segment files.
	SegmentCacheSize: 25,       // Number of cached in-memory segments
	NoCopy:           false,    // Make a new copy of data for every Read call.
	UseMmap:          true,     // use mmap for faster write block to file.
}

// BlockFile represents a block to rfile
type BlockFile struct {
	mu              sync.RWMutex
	path            string            // absolute path to log directory
	tmpPath         string            // absolute tmpPath to log directory, we can use this tmpPath to accelerate IO
	opts            Options           // log options
	closed          bool              // log is closed
	corrupt         bool              // log may be corrupt
	lastSegment     *segment          // last log segment
	lastIndex       uint64            // index of the last entry in log
	sfile           *tbf.LockableFile // tail segment rfile handle
	wbatch          Batch             // reusable write batch
	logger          protocol.Logger
	bclock          time.Time
	cachedBuf       []byte
	openedFileCache tinylru.LRU   // openedFile entries cache
	readTimeOut     time.Duration // read block file default time out
	stopCh          chan struct{}
}

// segment represents a single segment rfile.
type segment struct {
	path  string     // path of segment rfile
	name  string     // name of segment rfile
	index uint64     // first index of segment
	ebuf  []byte     // cached entries buffer, storage format of one log entry: checksum|data_size|data
	epos  []tbf.Bpos // cached entries positions in buffer
}

// Open a new write ahead log
//
//	@Description:
//	@param path
//	@param opts
//	@param logger
//	@return *BlockFile
//	@return error
func Open(path, tmpPath string, opts *Options, logger protocol.Logger) (*BlockFile, error) {
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

	useTmpFile := false
	if len(tmpPath) > 0 {
		if tmpPath, err = filepath.Abs(tmpPath); err == nil {
			if err = os.MkdirAll(tmpPath, os.ModePerm); err != nil {
				return nil, err
			}
			if err = utils.CheckPathRWMod(tmpPath); err == nil {
				useTmpFile = true
			}
		}
	}

	if !useTmpFile {
		tmpPath = ""
		logger.Infof("tmpPath is invalidate, will write block to path directly, err: %v", err)
	}

	rTimeOut := 10000 * time.Millisecond
	if opts.ReadTimeOut > 0 {
		rTimeOut = time.Duration(opts.ReadTimeOut) * time.Millisecond
	}

	l := &BlockFile{
		path:        path,
		tmpPath:     tmpPath,
		opts:        *opts,
		logger:      logger,
		cachedBuf:   make([]byte, 0, int(float32(opts.SegmentSize)*float32(1.5))),
		readTimeOut: rTimeOut,
		stopCh:      make(chan struct{}),
	}

	l.openedFileCache.Resize(l.opts.SegmentCacheSize)
	if err = os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}
	if err = utils.CheckPathRWMod(path); err != nil {
		return nil, err
	}
	if err = l.load(); err != nil {
		return nil, err
	}

	if useTmpFile && l.useTmpPath() {
		go l.moveFDBFile()
	}

	return l, nil
}

// pushCache  add segFile to openedFileCache and remove segFile more than cache capacity
//
//	@Description:
//	@receiver l
//	@param path
//	@param segFile
func (l *BlockFile) pushCache(path string, segFile *tbf.LockableFile) {
	if strings.HasSuffix(path, tbf.LastFileSuffix) {
		// has LastFileSuffix mean this is a writing rfile, only happened when add new block entry to rfile,
		// we do not use ofile in other place, since we read new added block entry from memory (l.lastSegment.ebuf),
		// so we should not cache this rfile object
		return
	}
	_, _, _, v, evicted :=
		l.openedFileCache.SetEvicted(path, segFile)
	if evicted {
		// nolint
		if v == nil {
			return
		}
		if lfile, ok := v.(*tbf.LockableFile); ok {
			lfile.Lock()
			if lfile.Wfile != nil {
				_ = lfile.Wfile.Close()
			}
			if lfile.Rfile != nil {
				_ = lfile.Rfile.Close()
			}
			lfile.Unlock()
		}
	}
}

// load
// @Description: load all the segments. This operation also cleans up any START/END segments.
// @receiver l
// @return error
func (l *BlockFile) load() error {
	var err error
	if err = l.loadFromPath(); err != nil {
		return err
	}

	// for the first time to start
	if l.lastSegment == nil {
		// Create a new log
		segName := tbf.Uint64ToSegmentName(1)
		l.lastSegment = &segment{
			name:  segName,
			index: 1,
			path:  l.segmentPathWithENDSuffix(segName),
			ebuf:  l.cachedBuf[:0],
			epos:  make([]tbf.Bpos, 0),
		}
		l.lastIndex = 0
		l.sfile, err = tbf.OpenWriteFile(l.lastSegment.path, tbf.LastFileSuffix,
			l.opts.UseMmap, l.opts.SegmentSize, l.logger)
		return err
	}

	// Open the last segment for appending
	if l.sfile, err = tbf.OpenWriteFile(l.lastSegment.path, tbf.LastFileSuffix,
		l.opts.UseMmap, l.opts.SegmentSize, l.logger); err != nil {
		return err
	}

	// Customize part start
	// Load the last segment, only load uncorrupted log entries
	s := l.lastSegment
	s.ebuf, s.epos, err = tbf.LoadSegmentEntriesForRestarting(s.path, s.index)
	if err != nil {
		return err
	}
	// Customize part end
	l.lastIndex = l.lastSegment.index + uint64(len(l.lastSegment.epos)) - 1
	return nil
}

// loadFromPath
// @Description: 从指定目录加载segment/文件
// @receiver l
// @param path
// @return error
func (l *BlockFile) loadFromPath() error {
	pat := l.path
	if l.useTmpPath() {
		pat = l.tmpPath
	}
	fis, err := ioutil.ReadDir(pat)
	if err != nil {
		return err
	}
	var index, endFile, maxIndex uint64
	// during the restart, wal files are loaded to log.segments
	for _, fi := range fis {
		name := fi.Name()
		if fi.IsDir() {
			continue
		}

		if !tbf.CheckSegmentName(name) {
			continue
		}

		// mv .fdb file from tmpPath to pat
		if l.useTmpPath() && strings.HasSuffix(name, tbf.DBFileSuffix) {
			if err1 := l.copyThenRemoveFile(
				filepath.Join(l.tmpPath, name),
				filepath.Join(l.path, name)); err1 != nil {
				return err1
			}
			continue
		}

		index, err = strconv.ParseUint(name[1:tbf.DBFileNameLen], 10, 64) // index most time is the first height of bfdb rfile
		if err != nil || index == 0 {
			continue
		}

		if index > maxIndex {
			maxIndex = index
		}

		if strings.HasSuffix(name, tbf.LastFileSuffix) && endFile < index {
			endFile = index
		}
	}

	segName := ""
	// use the max endFile
	if endFile > 0 {
		segName = tbf.Uint64ToSegmentName(endFile)
		index = endFile
	} else if endFile == 0 && maxIndex > 0 {
		// can only find .fdb files instead of .END file, we need fix this encounter by rename last .fdb to .fdb.END
		segName = tbf.Uint64ToSegmentName(maxIndex)
		fdbPath := tbf.SegmentPath(segName, pat)
		endPath := l.segmentPathWithENDSuffix(segName)
		l.logger.Warnf("can not find .END file, will move %s to %s as END file", fdbPath, endPath)
		if err1 := os.Rename(fdbPath, endPath); err1 != nil {
			return err1
		}
		index = maxIndex
	}

	if len(segName) == tbf.DBFileNameLen {
		l.lastSegment = &segment{
			name:  segName,
			index: index,
			path:  l.segmentPathWithENDSuffix(segName),
			ebuf:  l.cachedBuf[:0],
			epos:  make([]tbf.Bpos, 0),
		}
	}

	return nil
}

// openReadFile open read file from path then add opened file to openedFileCache
// @Description:
// @receiver l
// @param path
// @return *lockableFile
// @return error
func (l *BlockFile) openReadFile(path string) (*tbf.LockableFile, error) {
	// Open the appropriate rfile as read-only.
	var (
		err     error
		isExist bool
		rfile   *os.File
		ofile   *tbf.LockableFile
	)

	fileV, isOK := l.openedFileCache.Get(path)
	if isOK && fileV != nil {
		if isExist, _ = utils.PathExists(path); isExist {
			ofil, ok := fileV.(*tbf.LockableFile)
			if ok && ofil != nil && ofil.Rfile != nil {
				l.pushCache(path, ofil)
				return ofil, nil
			}
		}
	}

	if isExist, err = utils.PathExists(path); err != nil {
		return nil, err
	} else if !isExist {
		return nil, fmt.Errorf(rFileMissedMessageTemplate, path)
	}

	rfile, err = os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	if _, err = rfile.Seek(0, 2); err != nil {
		return nil, err
	}

	ofile = &tbf.LockableFile{Rfile: rfile}
	l.pushCache(path, ofile)
	return ofile, nil
}

// openReadFileWithTimeOut openReadFile with timeout feature
// @Description:
// @receiver l
// @param path
// @param timeOut
// @return *lockableFile
// @return error
func (l *BlockFile) openReadFileWithTimeOut(path string, timeOut time.Duration) (*tbf.LockableFile, error) {
	var (
		err    error
		lbFile *tbf.LockableFile
	)

	ch1 := make(chan int, 1)
	go func(ch chan<- int) {
		lbFile, err = l.openReadFile(path)
		ch <- 1
	}(ch1)
	timer := time.NewTimer(timeOut)

	select {
	case <-ch1:
		return lbFile, err
	case <-timer.C:
		l.logger.Warnf("open read file: %s, time out for %v", path, timeOut)
		err = fmt.Errorf("time out")
	}
	return nil, err
}

// segmentPathWithENDSuffix
//
//	@Description:
//	@receiver l
//	@param name
//	@return string
func (l *BlockFile) segmentPathWithENDSuffix(name string) string {
	if strings.HasSuffix(name, tbf.LastFileSuffix) {
		return name
	}
	pat := l.path
	if l.useTmpPath() {
		pat = l.tmpPath
	}

	return fmt.Sprintf("%s%s", tbf.SegmentPath(name, pat), tbf.LastFileSuffix)
}

// Close the log.
//
//	@Description:
//	@receiver l
//	@return error
func (l *BlockFile) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		if l.corrupt {
			return ErrCorrupt
		}
		return ErrClosed
	}
	if l.stopCh != nil {
		close(l.stopCh)
	}
	if err := l.sfile.Wfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Wfile.Close(); err != nil {
		return err
	}
	l.closed = true
	if l.corrupt {
		return ErrCorrupt
	}

	l.openedFileCache.Resize(l.opts.SegmentCacheSize)
	return nil
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
func (l *BlockFile) Write(index uint64, data []byte) (fileName string, offset, blkLen uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.bclock = time.Now()
	if l.corrupt {
		return "", 0, 0, ErrCorrupt
	} else if l.closed {
		return "", 0, 0, ErrClosed
	}
	l.wbatch.Write(index, data)
	bwi, err := l.writeBatch(&l.wbatch)
	if err != nil {
		return "", 0, 0, err
	}
	return bwi.FileName, bwi.Offset, bwi.ByteLen, nil
}

// cycle
// @Description: Cycle the old segment for a new segment.
// @receiver l
// @return error
func (l *BlockFile) cycle() error {
	if l.opts.NoSync {
		if err := l.sfile.Wfile.Sync(); err != nil {
			return err
		}
	}
	if err := l.sfile.Wfile.Close(); err != nil {
		return err
	}

	// remove pre last segment name's end suffix
	orgPath := l.lastSegment.path
	if !strings.HasSuffix(orgPath, tbf.LastFileSuffix) {
		return fmt.Errorf("last segment rfile dot not end with %s", tbf.LastFileSuffix)
	}
	finalPath := orgPath[:len(orgPath)-len(tbf.LastFileSuffix)]
	if err := os.Rename(orgPath, finalPath); err != nil {
		return err
	}

	// cache the previous lockfile
	l.pushCache(finalPath, l.sfile)

	segName := tbf.Uint64ToSegmentName(l.lastIndex + 1)
	s := &segment{
		name:  segName,
		index: l.lastIndex + 1,
		path:  l.segmentPathWithENDSuffix(segName),
		ebuf:  l.cachedBuf[:0],
		epos:  make([]tbf.Bpos, 0),
	}
	var err error
	if l.sfile, err = tbf.OpenWriteFile(s.path, tbf.LastFileSuffix, l.opts.UseMmap,
		l.opts.SegmentSize, l.logger); err != nil {
		return err
	}
	l.lastSegment = s
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
	index uint64
	size  int
}

// Write
// @Description: Write an entry to the batch
// @receiver b
// @param index
// @param data
func (b *Batch) Write(index uint64, data []byte) {
	b.entry = batchEntry{index, len(data)}
	b.data = data
}

// WriteBatch writes the entries in the batch to the log in the order that they
// were added to the batch. The batch is cleared upon a successful return.
//
//	@Description:
//	@receiver l
//	@param b
//	@return *storePb.StoreInfo
//	@return error
func (l *BlockFile) WriteBatch(b *Batch) (*storePb.StoreInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return nil, ErrCorrupt
	} else if l.closed {
		return nil, ErrClosed
	}
	if b.entry.size == 0 {
		return nil, nil
	}
	return l.writeBatch(b)
}

// writeBatch 写一个batch 返回索引信息
// @Description:
// @receiver l
// @param b
// @return *storePb.StoreInfo
// @return error
func (l *BlockFile) writeBatch(b *Batch) (*storePb.StoreInfo, error) {
	// check that indexes in batch are same
	if b.entry.index != l.lastIndex+uint64(1) {
		l.logger.Errorf(fmt.Sprintf("out of order, b.entry.index: %d and l.lastIndex+uint64(1): %d",
			b.entry.index, l.lastIndex+uint64(1)))
		if l.lastIndex == 0 {
			l.logger.Errorf("your block rfile db is damaged or not use blockfile before, " +
				"please check your disable_block_file_db setting in techtradechain.yml")
		}
		return nil, ErrOutOfOrder
	}

	// load the tail segment
	s := l.lastSegment
	if len(s.ebuf) > l.opts.SegmentSize {
		// tail segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return nil, err
		}
		s = l.lastSegment
	}

	var epos tbf.Bpos
	s.ebuf, epos = tbf.AppendBinaryEntry(s.ebuf, b.data)
	s.epos = append(s.epos, epos)

	startTime := time.Now()
	l.sfile.Lock()
	if _, err := l.sfile.Wfile.WriteAt(s.ebuf[epos.Pos:epos.End], int64(epos.Pos)); err != nil {
		l.logger.Errorf("write rfile: %s in %d err: %v", s.path, s.index+uint64(len(s.epos)), err)
		return nil, err
	}
	l.lastIndex = b.entry.index
	l.sfile.Unlock()
	l.logger.Debugf("writeBatch block[%d] rfile.WriteAt time: %v", l.lastIndex, utils.ElapsedMillisSeconds(startTime))

	if !l.opts.NoSync {
		if err := l.sfile.Wfile.Sync(); err != nil {
			return nil, err
		}
	}
	if epos.End-epos.Pos != b.entry.size+epos.PrefixLen {
		return nil, ErrBlockWrite
	}
	return &storePb.StoreInfo{
		FileName: l.lastSegment.name[:tbf.DBFileNameLen],
		Offset:   uint64(epos.Pos + epos.PrefixLen),
		ByteLen:  uint64(b.entry.size),
	}, nil
}

// LastIndex returns the index of the last entry in the log. Returns zero when
//
//	@Description:
//
// log has no entries.
//
//	@receiver l
//	@return index
//	@return err
func (l *BlockFile) LastIndex() (index uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return 0, ErrCorrupt
	} else if l.closed {
		return 0, ErrClosed
	}
	if l.lastIndex == 0 {
		return 0, nil
	}
	return l.lastIndex, nil
}

// ReadLastSegSection an entry from the log. Returns a byte slice containing the data entry.
//
//	@Description:
//	@receiver l
//	@param index
//	@return data
//	@return fileName
//	@return offset
//	@return byteLen
//	@return err
func (l *BlockFile) ReadLastSegSection(index uint64, forceFetch bool) (data []byte,
	fileName string, offset uint64, byteLen uint64, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.corrupt {
		return nil, "", 0, 0, ErrCorrupt
	} else if l.closed {
		return nil, "", 0, 0, ErrClosed
	}

	s := l.lastSegment
	if index == 0 || index < s.index || index > l.lastIndex {
		if forceFetch && index < s.index {
			l.logger.Infof("read rfile: %s index: %d not found in .END file, try read from .fdb", s.path, index)
			l.logger.Warnf("read entry from .fdb without fileIndex will cost much time and memory then has, " +
				"please reduce index (block height in techtradechain) distance between bfdb and kvdb by " +
				"upgrade your hardware condition !!!")
			return l.readFDBSegSection(index)
		}
		return nil, "", 0, 0, ErrNotFound
	}
	epos := s.epos[index-s.index]
	edata := s.ebuf[epos.Pos:epos.End]

	// Customize part start
	// checksum read
	checksum := binary.LittleEndian.Uint32(edata[:4])
	// binary read
	edata = edata[4:]
	// Customize part end
	size, n := binary.Uvarint(edata)
	if n <= 0 {
		return nil, "", 0, 0, ErrCorrupt
	}
	if uint64(len(edata)-n) < size {
		return nil, "", 0, 0, ErrCorrupt
	}
	// Customize part start
	if checksum != tbf.NewCRC(edata[n:]).Value() {
		return nil, "", 0, 0, ErrCorrupt
	}
	// Customize part end
	if l.opts.NoCopy {
		data = edata[n : uint64(n)+size]
	} else {
		data = make([]byte, size)
		copy(data, edata[n:])
	}
	fileName = l.lastSegment.name[:tbf.DBFileNameLen]
	offset = uint64(epos.Pos + epos.PrefixLen)
	byteLen = uint64(len(data))
	return data, fileName, offset, byteLen, nil
}

// doReadFileSection an entry from the log. Returns a byte slice containing the data entry.
// @Description:
// @receiver l
// @param fiIndex
// @return []byte
// @return error
func (l *BlockFile) doReadFileSection(fiIndex *storePb.StoreInfo, timeOut time.Duration) ([]byte, error) {
	if fiIndex == nil || len(fiIndex.FileName) != tbf.DBFileNameLen || fiIndex.ByteLen == 0 {
		l.logger.Warnf("invalidate file index: %s", tbf.FileIndexToString(fiIndex))
		return nil, ErrInvalidateIndex
	}
	pat := tbf.SegmentPath(fiIndex.FileName, l.path)
	lastFile := false
	l.mu.RLock()
	if fiIndex.FileName == l.lastSegment.name[:tbf.DBFileNameLen] {
		//pat = l.segmentPathWithENDSuffix(fiIndex.FileName)
		//lastFile = true
		dataBytes := make([]byte, fiIndex.ByteLen)
		startOffset := fiIndex.Offset
		endOffset := startOffset + fiIndex.ByteLen
		copy(dataBytes, l.lastSegment.ebuf[startOffset:endOffset])
		l.mu.RUnlock()
		return dataBytes, nil
	}
	l.mu.RUnlock()

	cnt := 0
	readingTmpFile := false
	l.logger.Debugf("read file section, pat: %s, offset: %d, dataLen: %d", pat, fiIndex.Offset, fiIndex.ByteLen)
	lfile, err := l.openReadFileWithTimeOut(pat, timeOut)
	if err != nil {
		l.logger.Warnf("read file section failed: %v", err)
		if lastFile {
			pat = tbf.SegmentPath(fiIndex.FileName, l.path)
			l.logger.Debugf("read file is last .END file, will try pat: %s", pat)
			if lfile, err = l.openReadFileWithTimeOut(pat, timeOut); err != nil {
				l.logger.Warnf("read pre .END file: %s section failed: %v", pat, err)
				return nil, err
			}
		} else if l.useTmpPath() {
			lfile = &tbf.LockableFile{}
			pat = tbf.SegmentPath(fiIndex.FileName, l.tmpPath)
			readingTmpFile = true
			if lfile.Rfile, err = os.OpenFile(pat, os.O_RDONLY, 0666); err != nil {
				return nil, err
			}
			if err = utils.FileLocker(lfile.Rfile); err != nil {
				_ = utils.FileUnLocker(lfile.Rfile)
				return nil, err
			}
			defer func() { _ = utils.FileUnLocker(lfile.Rfile) }()
			if _, err = lfile.Rfile.Seek(0, 2); err != nil {
				return nil, err
			}
		}
		if err != nil {
			l.logger.Warnf("read file section, open path: %s failed: %v", pat, err)
			l.openedFileCache.Delete(pat)
			return nil, err
		}
	}

	data := make([]byte, fiIndex.ByteLen)
	if readingTmpFile {
		cnt, err = lfile.Rfile.ReadAt(data, int64(fiIndex.Offset))
		_ = lfile.Rfile.Close()
	} else {
		lfile.RLock()
		cnt, err = lfile.Rfile.ReadAt(data, int64(fiIndex.Offset))
	}
	lfile.RUnlock()
	if err != nil {
		l.logger.Warnf("read file section, read pat: %s  data failed: %v", pat, err)
		return nil, err
	}
	if uint64(cnt) != fiIndex.ByteLen {
		return nil, fmt.Errorf("read block file size invalidate, wanted: %d, actual: %d", fiIndex.ByteLen, cnt)
	}
	return data, nil
}

// readFDBSegSection read the data from the .fdb file by index
// @Description:
// @receiver l
// @param index
// @return data
// @return fileName
// @return offset
// @return byteLen
// @return err
func (l *BlockFile) readFDBSegSection(index uint64) (data []byte,
	fileName string, offset uint64, byteLen uint64, err error) {
	sn, errs := l.findFDBFile(index)
	if errs != nil {
		return nil, "", 0, 0, errs
	}

	bpath := l.path
	if sn.IsTmp {
		bpath = l.tmpPath
	}
	data, err = ioutil.ReadFile(path.Join(bpath, sn.Name))
	if err != nil && sn.IsTmp {
		l.logger.Warnf("open tmp file %s err: %v, try bfdb path", sn.Name, err)
		data, err = ioutil.ReadFile(path.Join(l.path, sn.Name))
	}
	if err != nil {
		return nil, "", 0, 0, err
	}

	var (
		epos tbf.Bpos
		pos  int
	)
	ebuf := data
	for exidx := sn.Index; len(data) > 0; exidx++ {
		var n, prefixLen int
		n, prefixLen, err = tbf.LoadNextBinaryEntry(data)
		// if there are corrupted log entries, the corrupted and subsequent data are discarded
		if err != nil {
			break
		}
		data = data[n:]
		if exidx == index {
			epos = tbf.Bpos{Pos: pos, End: pos + n, PrefixLen: prefixLen}
			break
		}
		pos += n
	}
	byteLen = uint64(epos.End - epos.Pos - epos.PrefixLen)
	offset = uint64(epos.Pos + epos.PrefixLen)
	return ebuf[offset : offset+byteLen], sn.Name[:tbf.DBFileNameLen], offset, byteLen, nil
}

// findFDBFile find the .fdb file by index
// @Description:
// @receiver l
// @param index
// @return *segname
// @return error
func (l *BlockFile) findFDBFile(index uint64) (*tbf.SegName, error) {
	segns, err := l.filterFDB(l.path, false)
	if err != nil {
		return nil, err
	}

	if l.useTmpPath() {
		segs, errs := l.filterFDB(l.tmpPath, true)
		if errs != nil {
			return nil, errs
		}
		segns = append(segns, segs...)
	}

	// sort segns increasing order
	sort.Slice(segns, func(i, j int) bool {
		return segns[i].Index < segns[j].Index
	})

	// find the segment file in decreasing order
	var sn *tbf.SegName
	if segns[len(segns)-1].Index <= index {
		sn = segns[len(segns)-1]
	} else {
		for i := len(segns) - 1; i > 0; i-- {
			if segns[i-1].Index <= index && index < segns[i].Index {
				sn = segns[i-1]
				break
			}
		}
	}
	return sn, nil
}

// filterFDB filter then find the .fdb file
// @Description:
// @receiver l
// @param path
// @param isTmpPath
// @return []*tbf.SegName
// @return error
func (l *BlockFile) filterFDB(path string, isTmpPath bool) ([]*tbf.SegName, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var index uint64
	nameLen := tbf.DBFileNameLen + len(tbf.DBFileSuffix)
	sgeNames := make([]*tbf.SegName, 0, len(fis))
	for _, fi := range fis {
		name := fi.Name()
		if fi.IsDir() {
			continue
		}

		if strings.HasSuffix(name, tbf.DBFileSuffix) && len(name) == nameLen {
			index, err = strconv.ParseUint(name[1:tbf.DBFileNameLen], 10, 64)
			if err != nil || index == 0 {
				continue
			}
			sgeNames = append(sgeNames, &tbf.SegName{
				IsTmp: isTmpPath,
				Name:  name,
				Index: index,
			})
		}
	}

	return sgeNames, nil
}

// ReadFileSection add next time
// @Description:
// @receiver l
// @param fiIndex
// @param timeOut
// @return []byte
// @return error
func (l *BlockFile) ReadFileSection(fiIndex *storePb.StoreInfo, timeOut time.Duration) ([]byte, error) {
	var (
		err  error
		data []byte
	)

	if timeOut == 0 {
		timeOut = l.readTimeOut
	}

	ch1 := make(chan int, 1)
	go func(ch chan<- int) {
		data, err = l.doReadFileSection(fiIndex, timeOut)
		ch <- 1
	}(ch1)

	timer := time.NewTimer(timeOut)

	select {
	case <-ch1:
		return data, err
	case <-timer.C:
		l.logger.Warnf("read file section: %s, time out for %v", fiIndex.FileName, timeOut)
		err = fmt.Errorf("time out")
	}

	return nil, err
}

// ClearCache clears the segment cache
//
//	@Description:
//	@receiver l
//	@return error
func (l *BlockFile) ClearCache() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	l.clearCache()
	return nil
}

// clearCache 清除cache
// @Description:
// @receiver l
func (l *BlockFile) clearCache() {
	l.openedFileCache.Range(func(_, v interface{}) bool {
		// nolint
		if v == nil {
			return true
		}
		if s, ok := v.(*tbf.LockableFile); ok {
			s.Lock()
			_ = s.Rfile.Close()
			s.Unlock()
		}
		return true
	})
	l.openedFileCache = tinylru.LRU{}
	l.openedFileCache.Resize(l.opts.SegmentCacheSize)
}

// Sync performs a fsync on the log. This is not necessary when the
// NoSync option is set to false.
// @Description:
// @receiver l
// @return error
func (l *BlockFile) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.corrupt {
		return ErrCorrupt
	} else if l.closed {
		return ErrClosed
	}
	return l.sfile.Wfile.Sync()
}

// TruncateFront 清除数据
//
//	@Description:
//	@receiver l
//	@param index
//	@return error
func (l *BlockFile) TruncateFront(index uint64) error {
	return nil
}

// useTmpPath tmpPath flag
// @Description:
// @receiver l
// @return bool
func (l *BlockFile) useTmpPath() bool {
	return len(l.tmpPath) > 0
}

// moveFDBFile move fdb file from tmpPath to origin bfdb path.
// this func will scan tmpPath directory then trigger move action and wait 10 second
// @Description:
// @receiver l
func (l *BlockFile) moveFDBFile() {
	var err error
	interval := time.Second * 10

	for {
		select {
		case <-l.stopCh:
			l.logger.Infof("block file tmpPath move FDB files action closed")
			return
		default:
			if err = l.scanTmpPath(); err != nil {
				l.logger.Warnf("move fdb file failed, err: %v", err)
			}
			time.Sleep(interval)
		}
	}
}

// scanTmpPath scan tmpPath directory then move .fdb file
// @Description:
// @receiver l
// @return error
func (l *BlockFile) scanTmpPath() error {
	if !l.useTmpPath() {
		return nil
	}

	fis, err := ioutil.ReadDir(l.tmpPath)
	if err != nil {
		return err
	}
	// during the restart, wal files are loaded to log.segments
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}

		// mv .fdb file from tmpPath to path
		name := fi.Name()
		if !tbf.CheckSegmentName(name) {
			continue
		}
		if l.useTmpPath() && strings.HasSuffix(name, tbf.DBFileSuffix) {
			if err1 := l.copyThenRemoveFile(
				filepath.Join(l.tmpPath, name),
				filepath.Join(l.path, name)); err1 != nil {
				return err1
			}
		}
	}

	return nil
}

// copyThenRemoveFile  copy src file to dst file.
//
//	these two path may be located in different disk, so can not use 'mv' directly to prevent network or power issue
//
// @Description:
// @param src
// @param dst
// @return error
func (l *BlockFile) copyThenRemoveFile(src, dst string) error {
	var (
		err  error
		srcf *os.File
	)

	if srcf, err = l.getCachedFile(src, srcf); err != nil {
		return err
	}
	err = tbf.CopyThenRemoveFile(src, dst, srcf)
	if err != nil {
		l.openedFileCache.Delete(src)
		return err
	}
	l.openedFileCache.Delete(src)
	return nil
}

// DropOpenedFileCache drop opened file handle cache
// @Description:
// @receiver l
// @param path
// @return bool
func (l *BlockFile) DropOpenedFileCache(path string) bool {
	if len(path) == 0 {
		return true
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		pathAbs = path
	}
	_, ok := l.openedFileCache.Delete(pathAbs)
	return ok
}

// CheckFileExist check file exist
// @Description:
// @param fiIndex
// @return bool
// @return error
func (l *BlockFile) CheckFileExist(fiIndex *storePb.StoreInfo) (bool, error) {
	//if fiIndex == nil ||
	//	(!strings.HasPrefix(fiIndex.FileName, configFilePrefix) &&
	//		len(fiIndex.FileName) != DBFileNameLen) ||
	//	(strings.HasPrefix(fiIndex.FileName, configFilePrefix) &&
	//		len(fiIndex.FileName) != DBFileNameLen+configFilePrefixLen) ||
	//	fiIndex.ByteLen == 0 {
	//	l.logger.Warnf("invalidate file index: %s", FileIndexToString(fiIndex))
	//	return false, ErrInvalidateIndex
	//}

	if fiIndex == nil || len(fiIndex.FileName) != tbf.DBFileNameLen || fiIndex.ByteLen == 0 {
		l.logger.Warnf("invalidate file index: %s", tbf.FileIndexToString(fiIndex))
		return false, ErrInvalidateIndex
	}
	pat := tbf.SegmentPath(fiIndex.FileName, l.path)
	if fiIndex.FileName == l.lastSegment.name[:tbf.DBFileNameLen] {
		pat = l.segmentPathWithENDSuffix(fiIndex.FileName)
	}
	return utils.PathExists(pat)
}

// getCachedFile add next time
//
//	@Description:
//	@receiver l
//	@param src
//	@param srcf
//	@return *os.File
//	@return error
func (l *BlockFile) getCachedFile(src string, srcf *os.File) (*os.File, error) {
	var err error
	fileV, isOK := l.openedFileCache.Get(src)
	if isOK && fileV != nil {
		if isExist, _ := utils.PathExists(src); isExist {
			ofil, ok := fileV.(*tbf.LockableFile)
			if ok {
				if ofil.Wfile != nil {
					_ = ofil.Wfile.Close()
				}
				if ofil.Rfile != nil {
					_ = ofil.Rfile.Close()
				}
				if ofil.Rfile, err = os.Open(src); err != nil {
					return nil, err
				}
				srcf = ofil.Rfile
			}
		}
	}
	return srcf, err
}
