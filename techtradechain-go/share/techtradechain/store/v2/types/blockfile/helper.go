package blockfile

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"techtradechain.com/techtradechain/common/v2/json"
	lwsf "techtradechain.com/techtradechain/lws/file"
	storePb "techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/store/v2/utils"
)

// AppendBinaryEntry append an entry
//
//	@Description:
//	@receiver l
//	@param dst
//	@param data
//	@return out
//	@return epos
func AppendBinaryEntry(dst []byte, data []byte) (out []byte, epos Bpos) {
	// checksum + data_size + data
	pos := len(dst)
	// Customize part start
	dst = AppendChecksum(dst, NewCRC(data).Value())
	// Customize part end
	dst = AppendUvarint(dst, uint64(len(data)))
	prefixLen := len(dst) - pos
	dst = append(dst, data...)
	return dst, Bpos{pos, len(dst), prefixLen}
}

// AppendChecksum Customize part start
//
//	@Description:
//	@param dst
//	@param checksum
//	@return []byte
func AppendChecksum(dst []byte, checksum uint32) []byte {
	dst = append(dst, []byte("0000")...)
	binary.LittleEndian.PutUint32(dst[len(dst)-4:], checksum)
	return dst
}

// AppendUvarint Customize part end
//
//	@Description:
//	@param dst
//	@param x 整型序列化
//	@return []byte
func AppendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

// FileIndexToString convert
//
//	@Description:
//	@param fiIndex
//	@return string
func FileIndexToString(fiIndex *storePb.StoreInfo) string {
	if fiIndex == nil {
		return "rfile index is nil"
	}

	return fmt.Sprintf("fileIndex: fileName: %s, offset: %d, byteLen: %d",
		fiIndex.GetFileName(), fiIndex.GetOffset(), fiIndex.GetByteLen())
}

// Uint64ToSegmentName returns a 20-byte textual representation of an index
// for lexical ordering. This is used for the rfile names of log segments.
//
//	@Description:
//	@receiver l
//	@param index
//	@return string
func Uint64ToSegmentName(index uint64) string {
	return fmt.Sprintf(BlockFilenameTemplate, index)
}

// SegmentNameToUint64 add next time
// @Description:
// @param fileName
// @return uint64
// @return error
func SegmentNameToUint64(fileName string) (uint64, error) {
	return strconv.ParseUint(fileName[1:DBFileNameLen], 10, 64)
}

// SegmentPath build segment dir path
//
//	@Description:
//	@receiver l
//	@param name
//	@return string
func SegmentPath(name, path string) string {
	return fmt.Sprintf("%s%s", filepath.Join(path, name), DBFileSuffix)
}

// SegmentIndexPath build segment index dir path
//
//	@Description:
//	@receiver l
//	@param name
//	@return string
func SegmentIndexPath(name, path string) string {
	return fmt.Sprintf("%s%s", filepath.Join(path, name), RestoringIndexFileName)
}

//// SegmentName returns a 20-byte textual representation of an index
//// for lexical ordering. This is used for the rfile names of log segments.
////  @Description:
////  @receiver l
////  @param index
////  @return string
//func SegmentName(index uint64) string {
//	return fmt.Sprintf(BlockFilenameTemplate, index)
//}

// CheckSegmentName add next time
// @Description:
// @param name
// @return bool
func CheckSegmentName(name string) bool {
	if strings.Index(name, ".") != DBFileNameLen {
		return false
	}

	sufs := []string{
		DBFileSuffix,
		LastFileSuffix,
		ArchivingBlockFileName,
		ArchivingResultFileName,
		RestoringIndexFileName,
		MergingFileSuffix,
		BakFileSuffix,
	}

	for _, suf := range sufs {
		if strings.HasSuffix(name, suf) {
			return true
		}
	}

	return false
}

// OpenWriteFile open block file write file
//
//	@Description:
//	@receiver l
//	@param path
//	@return *lockableFile
//	@return error
func OpenWriteFile(path, fileSuffix string, useMmap bool, segSize int, log protocol.Logger) (*LockableFile, error) {
	var (
		err     error
		isExist bool
		wfile   lwsf.WalFile
	)

	if isExist, err = utils.PathExists(path); err != nil {
		return nil, err
	}

	if !isExist && !strings.HasSuffix(path, fileSuffix) {
		return nil, fmt.Errorf("bfdb wfile:%s missed", path)
	}

	if useMmap {
		if wfile, err = lwsf.NewMmapFile(path, segSize); err != nil {
			log.Warnf("failed use mmap: %v", err)
		}
	}

	if wfile == nil || err != nil {
		if wfile, err = lwsf.NewFile(path); err != nil {
			return nil, err
		}
	}

	return &LockableFile{Wfile: wfile}, nil
}

// LoadSegmentEntriesForRestarting loads ebuf and epos in the segment when restarting
//
//	@Description:
//	@receiver l
//	@param s
//	@return error
func LoadSegmentEntriesForRestarting(path string, index uint64) ([]byte, []Bpos, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	var (
		epos []Bpos
		pos  int
	)
	ebuf := data
	for exidx := index; len(data) > 0; exidx++ {
		var n, prefixLen int
		n, prefixLen, err = LoadNextBinaryEntry(data)
		// if there are corrupted log entries, the corrupted and subsequent data are discarded
		if err != nil {
			break
		}
		data = data[n:]
		epos = append(epos, Bpos{Pos: pos, End: pos + n, PrefixLen: prefixLen})
		pos += n
		// load uncorrupted data
		ebuf = ebuf[:pos]
	}

	return ebuf, epos, nil
}

// LoadEntriesForRestarting loads ebuf and epos in the segment when restarting
//
//	@Description:
//	@receiver l
//	@param s
//	@return error
func LoadEntriesForRestarting(path string) ([]byte, []Bpos, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	var (
		epos []Bpos
		pos  int
	)
	ebuf := data
	for len(data) > 0 {
		var n, prefixLen int
		n, prefixLen, err = LoadNextBinaryEntry(data)
		// if there are corrupted log entries, the corrupted and subsequent data are discarded
		if err != nil {
			break
		}
		data = data[n:]
		epos = append(epos, Bpos{Pos: pos, End: pos + n, PrefixLen: prefixLen})
		pos += n
		// load uncorrupted data
		ebuf = ebuf[:pos]
	}

	return ebuf, epos, nil
}

// LoadNextBinaryEntry 读取entry,返回
//
//	@Description:
//	@param data
//	@return n
//	@return prefixLen
//	@return err
func LoadNextBinaryEntry(data []byte) (n, prefixLen int, err error) {
	// Customize part start
	// checksum + data_size + data
	// checksum read
	logErr := fmt.Errorf("log corrupt")
	if len(data) < 4 {
		return 0, 0, logErr
	}
	checksum := binary.LittleEndian.Uint32(data[:4])
	// binary read
	data = data[4:]

	// Customize part end
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, logErr
	}
	if uint64(len(data)-n) < size {
		return 0, 0, logErr
	}
	// Customize part start
	// verify checksum
	if checksum != NewCRC(data[n:uint64(n)+size]).Value() {
		return 0, 0, logErr
	}
	prefixLen = 4 + n
	return prefixLen + int(size), prefixLen, nil
	// Customize part end
}

// AppendFileEntry add next time
// @Description:
// @receiver l
// @param s
// @return error
func AppendFileEntry(data []byte, fw *FileWriter, dataSize int, logger protocol.Logger) error {
	startTime := time.Now()
	fw.Lfile.Lock()
	var epos Bpos
	fw.Ebuf, epos = AppendBinaryEntry(fw.Ebuf, data)
	fw.Epos = append(fw.Epos, epos)
	_, err := fw.Lfile.Wfile.WriteAt(fw.Ebuf[epos.Pos:epos.End], int64(epos.Pos))
	if err != nil {
		logger.Errorf("write file: %s %d st entry err: %v", fw.Path, len(fw.Epos), err)
	}
	fw.Lfile.Unlock()

	if epos.End-epos.Pos != dataSize+epos.PrefixLen {
		return fmt.Errorf("write data to file %s %d st err, actual %d, expect: %d",
			fw.Path, len(fw.Epos), epos.End-epos.Pos, dataSize+epos.PrefixLen)
	}

	logger.Debugf("writeBatch entry (len:%d) to file:%s rfile.WriteAt time: %v", len(data),
		fw.Path, utils.ElapsedMillisSeconds(startTime))
	return nil
}

// CopyThenRemoveFile  copy src file to dst file.
//
//	these two path may be located in different disk, so can not use 'mv' directly to prevent network or power issue
//
// @Description:
// @param src
// @param dst
// @return error
func CopyThenRemoveFile(src, dst string, srcf *os.File) error {
	var (
		err     error
		srcStat os.FileInfo
	)

	if srcf == nil {
		srcStat, err = os.Stat(src)
		if err != nil {
			return err
		}
		_, err = os.Stat(filepath.Join(dst, "../"))
		if err != nil {
			return err
		}

		if srcf, err = os.Open(src); err != nil {
			return err
		}
		if err = utils.FileLocker(srcf); err != nil {
			_ = utils.FileUnLocker(srcf)
			return err
		}
		defer func() { _ = utils.FileUnLocker(srcf) }()
	}
	defer srcf.Close()

	dstf, err2 := os.Create(dst)
	if err2 != nil {
		return err
	}
	if err = utils.FileLocker(dstf); err != nil {
		_ = utils.FileUnLocker(srcf)
		return err
	}
	defer func() { _ = utils.FileUnLocker(dstf) }()
	defer dstf.Close()

	// copy file
	var written int64
	for count := 1; count <= 3; count++ {
		if written, err = io.Copy(dstf, srcf); err != nil {
			if count < 3 {
				err = nil
			}
			// we can add wait action if you need
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	if err != nil {
		return err
	}

	if srcStat != nil && written != srcStat.Size() {
		return fmt.Errorf("copy file %s[len: %d] to file: %s[len: %d] uncompleted",
			src, srcStat.Size(), dst, written)
	}

	// remove src file
	return os.Remove(src)
}

// LoadArchiveSegIndex add next time
// @Description:
// @receiver l
// @param s
// @return error
func LoadArchiveSegIndex(fw *FileWriter) ([]*SegIndex, error) {
	var err error
	fw.Ebuf, fw.Epos, err = LoadEntriesForRestarting(fw.Path)
	if err != nil {
		return nil, err
	}

	segIndexs := make([]*SegIndex, 0, len(fw.Epos))
	for _, ep := range fw.Epos {
		var segI SegIndex
		if err = json.Unmarshal(fw.Ebuf[ep.Pos+ep.PrefixLen:ep.End], &segI); err != nil {
			return nil, err
		}
		segIndexs = append(segIndexs, &segI)
	}

	return segIndexs, nil
}

// NewFileWriter add next time
// @Description:
// @param path
// @return *FileWriter
// @return error
func NewFileWriter(path string) (*FileWriter, error) {
	var err error
	fw := &FileWriter{
		Path:  path,
		Lfile: &LockableFile{},
		Ebuf:  []byte{},
		Epos:  make([]Bpos, 0),
	}

	fw.Lfile.Wfile, err = lwsf.NewFile(fw.Path)
	return fw, err
}
