package blockfile

import (
	"errors"
	"os"
	"sync"

	lwsf "techtradechain.com/techtradechain/lws/file"
	"techtradechain.com/techtradechain/store/v2/serialization"
)

// nolint
const (
	DBFileSuffix          = ".fdb"
	DBFileNameLen         = 20
	BlockFilenameTemplate = "%020d"
	LastFileSuffix        = ".END"
)

// nolint
const (
	ArchivingPath           = "archiving"
	RestoringPath           = "restoring"
	ArchivingBlockFileName  = ".block.archiving.json"
	ArchivingResultFileName = ".result.archiving.json"
	RestoringIndexFileName  = ".restoring.json"
	MergingFileSuffix       = ".merging"
	BakFileSuffix           = ".bak"
)

var (
	//ErrInvalidateRestoreBlocks invalidate restore blocks
	ErrInvalidateRestoreBlocks = errors.New("invalidate restore blocks")
	//ErrConfigBlockArchive config block do not need to archive
	ErrConfigBlockArchive = errors.New("config block do not need archive")
	//ErrArchivedTx archived transaction
	ErrArchivedTx = errors.New("archived transaction")
	//ErrArchivedRWSet archived RWSet
	ErrArchivedRWSet = errors.New("archived RWSet")
	//ErrArchivedBlock archived block
	ErrArchivedBlock = errors.New("archived block")
)

// LockableFile add next time
// @Description:
type LockableFile struct {
	sync.RWMutex

	Wfile lwsf.WalFile
	Rfile *os.File
}

// Bpos byte position info
// @Description:
type Bpos struct {
	Pos       int // byte position
	End       int // one byte past pos
	PrefixLen int
}

// SegName segment name
// @Description:
type SegName struct {
	IsTmp bool
	Name  string
	Index uint64
}

// FileWriter block file db file writer
// @Description:
type FileWriter struct {
	Path  string
	Lfile *LockableFile // index file handle
	Ebuf  []byte        // cached entries buffer, storage format of one log entry: checksum|data_size|data
	Epos  []Bpos        // cached entries positions in buffer
}

// SegIndex restore segment index structure
// @Description:
type SegIndex struct {
	Height uint64
	BIndex *serialization.BlockIndexMeta
}

// IndexPair index pair to contain update batch data
type IndexPair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}
