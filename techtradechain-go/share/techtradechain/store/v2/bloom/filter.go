/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bloom

import (
	"io"
	"time"
	"unsafe"

	"github.com/bits-and-blooms/bloom/v3"
)

const (
	// KiB 表示一个千字节（KiB），即1024字节
	KiB = 1024
	// MiB 表示兆字节，即千兆比特 通过将 KiB 乘以 1024，我们可以得到 MiB
	MiB                = KiB * 1024
	headerLen          = 24
	arrayMaxLen        = 64 << 30
	worldBytes         = 8
	log2WordBytes      = 3
	maxWriteEntryBytes = 64 * MiB
	writeEntryWaitTime = 1 * time.Second
)

// BloomFilter a bloom filter with dump file
type BloomFilter struct {
	*bloom.BloomFilter
	file          *bloomFile
	entries       uint
	fp            float64
	writeSize     int64
	lastDumpLabel uint64
}

func newBloomFilter(n uint, fp float64, file string) (*BloomFilter, error) {
	if file == "" {
		return nil, ErrInvalidFile
	}
	filter := &BloomFilter{
		entries: n,
		fp:      fp,
	}
	if err := filter.load(file); err != nil {
		return nil, err
	}
	return filter, nil
}

// ItemCapacity get the Pre allocated item capacity
func (f *BloomFilter) ItemCapacity() uint {
	return f.entries
}

// FP get the false positive probability
func (f *BloomFilter) FP() float64 {
	return f.fp
}

// Dump dump bloom filter data to file with label as mark.
func (f *BloomFilter) Dump(label uint64) error {
	if err := f.file.dump(label, f); err != nil {
		return err
	}
	f.lastDumpLabel = label
	return nil
}

// Copy copy bloom filter
func (f *BloomFilter) Copy() *BloomFilter {
	filter := *f
	filter.BloomFilter = f.BloomFilter.Copy()
	return &filter
}

// LastDumpLabel get the last dump label
func (f *BloomFilter) LastDumpLabel() uint64 {
	return f.lastDumpLabel
}

// load bloom filter data from file, if data not exist, init a new bloom filter
func (f *BloomFilter) load(file string) error {
	fi, err := newBloomFile(file)
	if err != nil {
		return err
	}
	f.file = fi
	loaded, err := f.loadFromFile(fi)
	if err != nil {
		return err
	}
	if !loaded {
		f.init()
	}
	return nil
}

// loadFromFile load bloom filter data from bloom file.
// The id of loader is 0, which means no data needs to be loaded.
// Otherwise read data from loader and restore filter.
func (f *BloomFilter) loadFromFile(file *bloomFile) (bool, error) {
	loader, err := file.load()
	if err != nil {
		return false, err
	}
	if loader.ID() == 0 {
		return false, nil
	}
	if _, err := f.ReadFrom(loader); err != nil {
		return false, err
	}
	f.lastDumpLabel, f.entries, f.fp, f.writeSize =
		loader.Label(), loader.Entries(), loader.FP(), loader.Size()
	return true, nil
}

func (f *BloomFilter) init() {
	f.BloomFilter = bloom.NewWithEstimates(f.entries, f.fp)
	f.writeSize, _ = f.WriteTo(io.Discard)
}

// WriteTo write bloom filter data to the writer, including basic data and valid data.
// Return the number of bytes written.
// Returns an error encountered.
func (f *BloomFilter) WriteTo(w io.Writer) (int64, error) {
	n1, err := f.writeHeader(w)
	if err != nil {
		return int64(n1), err
	}
	n2, err := f.writeBody(w)
	return int64(n1 + n2), err
}

// writeHeader write the m, k, size of the bloom filter to writer.
// m is the total number of bits used by the bloom filter.
// k is the number of hash functions used by the bloom filter
// size is the total number of bytes used by the bloom filter
func (f *BloomFilter) writeHeader(w io.Writer) (int, error) {
	m, k := f.Cap(), f.K()
	headBz := [headerLen]byte{}
	*(*uint64)(unsafe.Pointer(&headBz[0])) = uint64(m)
	*(*uint64)(unsafe.Pointer(&headBz[8])) = uint64(k)
	*(*uint64)(unsafe.Pointer(&headBz[16])) = uint64(len(f.BitSet().Bytes()) * worldBytes)
	return w.Write(headBz[:])
}

// writeBody convert the underlying uint64 slice to a byte array and write it to writer.
func (f *BloomFilter) writeBody(w io.Writer) (int, error) {
	uint64Sl := f.BitSet().Bytes()
	bytesLen := len(uint64Sl) << log2WordBytes
	bytes := (*(*[arrayMaxLen]byte)(unsafe.Pointer(&uint64Sl[0])))[:bytesLen:bytesLen]
	if ws, ok := w.(WriteSyncer); ok && bytesLen > maxWriteEntryBytes {
		//需要分批写
		entryStart, entryEnd := 0, maxWriteEntryBytes
		for ; entryEnd <= bytesLen; entryStart, entryEnd = entryStart+maxWriteEntryBytes, entryEnd+maxWriteEntryBytes {
			if _, err := ws.Write(bytes[entryStart:entryEnd]); err != nil {
				return entryStart, err
			}
			if err := ws.Sync(); err != nil {
				return entryEnd, err
			}
			time.Sleep(writeEntryWaitTime)
		}
		if entryStart < bytesLen {
			if _, err := ws.Write(bytes[entryStart:]); err != nil {
				return entryStart, err
			}
		}
		return bytesLen, nil
	}
	return w.Write(bytes)
}

// ReadFrom read the basic info(m,k,size) from reader firstly.
// Then read size-sized bytes of data from a file.
// Rebuild a bloom filter with m, k and a uint64 slice converted from bytes slice.
func (f *BloomFilter) ReadFrom(r io.Reader) (int64, error) {
	m, k, s, err := f.readHeader(r)
	if err != nil {
		return 0, err
	}
	byteSl := make([]byte, s)
	if _, err := r.Read(byteSl); err != nil {
		return 0, err
	}
	uint64SlLen := s >> log2WordBytes
	uint64Sl := (*(*[arrayMaxLen]uint64)(unsafe.Pointer(&byteSl[0])))[:uint64SlLen:uint64SlLen]
	f.BloomFilter = bloom.FromWithM(uint64Sl[:], uint(m), uint(k))
	return int64(s + 24), nil
}

func (f *BloomFilter) readHeader(r io.Reader) (uint64, uint64, uint64, error) {
	headBz := [headerLen]byte{}
	if _, err := r.Read(headBz[:]); err != nil {
		return 0, 0, 0, err
	}
	return *(*uint64)(unsafe.Pointer(&headBz[0])),
		*(*uint64)(unsafe.Pointer(&headBz[8])),
		*(*uint64)(unsafe.Pointer(&headBz[16])),
		nil
}

// Close close the bloom filter
func (f *BloomFilter) Close() error {
	return f.file.close()
}
