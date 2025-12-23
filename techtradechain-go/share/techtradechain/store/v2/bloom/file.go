/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package bloom

import (
	"hash/fnv"
	"io"
	"os"
	"runtime"
	"time"
	"unsafe"
)

type chunkIdx int

const (
	headerChunkIdx = iota
	meta1ChunkIdx
	meta2ChunkIdx
)

const (
	magic   uint32 = 0x134D6F0
	version int64  = 1
)

// WriteSyncer is the interface that wraps basic Sync method, which
type WriteSyncer interface {
	io.Writer
	// Sync commits the current contents of the file to stable storage.
	Sync() error
}

type fileLock interface {
	release() error
}

type fileHeader struct {
	magic uint32
	// littleEndian uint32
	version  int64
	pageSize int64 //系统页大小
	checksum uint64
}
type bloomMeta struct {
	id       uint64
	label    uint64
	items    uint64
	fp       float64
	offset   int64
	size     int64 //数据长度
	checksum uint64
}

type bloomFile struct {
	path     string
	header   *fileHeader
	meta     [2]*bloomMeta
	file     *os.File
	fileLock fileLock
	id       uint64
}

func newBloomFile(path string) (*bloomFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	file := &bloomFile{
		path: path,
		file: f,
	}
	defer func() {
		if err != nil {
			_ = file.close()
		}
	}()
	var flock fileLock
	if flock, err = newFileLock(file, true, 100*time.Millisecond); err != nil {
		return nil, err
	}
	file.fileLock = flock
	if err = file.init(); err != nil {
		return nil, err
	}
	if err = file.validate(); err != nil {
		return nil, err
	}
	runtime.SetFinalizer(file, (*bloomFile).close)
	return file, nil
}

func (f *bloomFile) init() error {
	fs, err := f.file.Stat()
	if err != nil {
		return err
	}
	size := fs.Size()
	if size == 0 {
		return f.born()
	}
	var buf headerMetaBuf
	_, err = f.file.ReadAt(buf[:], 0)
	if err != nil && err != io.EOF {
		return err
	}
	f.header = buf.header()
	f.meta[0], f.meta[1] = buf.meta1(), buf.meta2()
	return nil
}

func (f *bloomFile) born() error {
	// var littleEndian uint32 = 0
	// if isLittleEndian() {
	// 	littleEndian = 1
	// }
	h := &fileHeader{
		magic: magic,
		// littleEndian: littleEndian,
		version:  version,
		pageSize: int64(os.Getpagesize()),
	}
	h.checksum = h.sum64()
	if err := f.writeHeader(h); err != nil {
		return err
	}
	f.header = h
	for i := 0; i < 2; i++ {
		meta := &bloomMeta{}
		meta.checksum = meta.sum64()
		f.meta[i] = meta
		chunk := newEmptyChunk()
		meta.copy(chunk.meta())
		if err := f.writeChunk(chunk, chunkIdx(i+1)); err != nil {
			return err
		}
	}
	return nil
}

func (f *bloomFile) validate() error {
	if err := f.header.validate(); err != nil {
		return err
	}
	meta := f.bloomMeta()
	if meta == nil {
		return ErrMetaAllInvalid
	}
	f.id = meta.id
	return nil
}

func (f *bloomFile) dump(label uint64, filter *BloomFilter) error {
	id := f.id + 1
	meta := &bloomMeta{
		id:    id,
		label: label,
		items: uint64(filter.entries),
		fp:    filter.fp,
		size:  filter.writeSize,
	}
	meta.offset = f.dataOffset(meta)
	meta.checksum = meta.sum64()
	if err := f.dumpData(meta, filter); err != nil {
		return err
	}
	f.id++
	return nil
}

func (f *bloomFile) dataOffset(meta *bloomMeta) int64 {
	return int64((meta.id-1)%2)*f.pageAlignUp(meta.size) + f.header.pageSize
}

func (f *bloomFile) pageAlignUp(n int64) int64 {
	return n + (f.header.pageSize-1)&^int64(f.header.pageSize-1)
}

func (f *bloomFile) load() (*BloomLoader, error) {
	meta := f.bloomMeta()
	if meta == nil || meta.id != f.id {
		return nil, ErrMetaAllInvalid
	}
	if meta.id == 0 {
		return &BloomLoader{
			meta: meta,
		}, nil
	}
	return &BloomLoader{
		Reader: io.LimitReader(newSeekReader(f.file, meta.offset), meta.size),
		meta:   meta,
	}, nil
}

func (f *bloomFile) bloomMeta() *bloomMeta {
	meta1 := f.meta[0]
	meta2 := f.meta[1]
	if meta1.id < meta2.id {
		meta1, meta2 = meta2, meta1
	}
	if err := meta1.validate(); err == nil {
		return meta1
	}
	if err := meta2.validate(); err == nil {
		return meta2
	}
	// panic("meta can't both invalid")
	return nil
}

func (f *bloomFile) writeHeader(h *fileHeader) error {
	chunk := newEmptyChunk()
	h.copy(chunk.header())
	return f.writeChunk(chunk, headerChunkIdx)
}

func (f *bloomFile) writeMeta(meta *bloomMeta) error {
	idx := (meta.id - 1) % 2
	chunk := newEmptyChunk()
	meta.copy(chunk.meta())
	return f.writeChunk(chunk, chunkIdx(idx+1))
}

func (f *bloomFile) flush() error {
	return f.file.Sync()
}

func (f *bloomFile) close() error {
	if f.fileLock != nil {
		_ = f.fileLock.release()
	}
	return f.file.Close()
}

func (f *bloomFile) dumpData(meta *bloomMeta, filter *BloomFilter) error {
	// fmt.Printf("dump data %+v\n", *meta)
	if _, err := filter.WriteTo(newSeekWriter(f.file, meta.offset)); err != nil {
		return err
	}
	if err := f.writeMeta(meta); err != nil {
		return err
	}
	if err := f.flush(); err != nil {
		return err
	}
	meta.copy(f.meta[(meta.id-1)%2])
	return nil
}

func (f *bloomFile) writeChunk(k *chunk, idx chunkIdx) error {
	buf := (*[chunkSize]byte)(unsafe.Pointer(k))[:]
	_, err := f.file.WriteAt(buf, int64(idx*chunkSize))
	return err
}

type headerMetaBuf [0x1000]byte

func (buf *headerMetaBuf) chunk(idx chunkIdx) *chunk {
	return (*chunk)(unsafe.Pointer(&buf[idx*chunkSize]))
}

func (buf *headerMetaBuf) header() *fileHeader {
	return buf.chunk(headerChunkIdx).header()
}

func (buf *headerMetaBuf) meta1() *bloomMeta {
	return buf.chunk(meta1ChunkIdx).meta()
}

func (buf *headerMetaBuf) meta2() *bloomMeta {
	return buf.chunk(meta2ChunkIdx).meta()
}

func (h *fileHeader) copy(to *fileHeader) {
	*to = *h
}

func (h *fileHeader) validate() error {
	if h.magic != magic {
		return ErrInvalidFile
	}
	if h.version != version {
		return ErrVersionMismatch
	}
	if h.checksum != h.sum64() {
		return ErrChecksum
	}
	return nil
}

func (h *fileHeader) sum64() uint64 {
	var hs = fnv.New64a()
	_, _ = hs.Write((*[unsafe.Offsetof(fileHeader{}.checksum)]byte)(unsafe.Pointer(h))[:])
	return hs.Sum64()
}

func (m *bloomMeta) copy(to *bloomMeta) {
	*to = *m
}

func (m *bloomMeta) validate() error {
	if m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

func (m *bloomMeta) sum64() uint64 {
	var hs = fnv.New64a()
	_, _ = hs.Write((*[unsafe.Offsetof(bloomMeta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return hs.Sum64()
}

// BloomLoader used to load bloom filter data from bloom file to recovery a bloom filter instance.
type BloomLoader struct {
	io.Reader
	meta *bloomMeta
}

// ID returns id of data saved. Zero means no data saved before.
func (l *BloomLoader) ID() uint64 {
	return l.meta.id
}

// Label return the label of data saved .
func (l *BloomLoader) Label() uint64 {
	return l.meta.label
}

// Entries get preset number of entries of the filter
func (l *BloomLoader) Entries() uint {
	return uint(l.meta.items)
}

// FP get preset false positive rate
func (l *BloomLoader) FP() float64 {
	return l.meta.fp
}

// Size get the size of data saved
func (l *BloomLoader) Size() int64 {
	return l.meta.size
}

// seekWriter wrapper for io.WriterAt
type seekWriter struct {
	*os.File
	offset int64
}

func newSeekWriter(f *os.File, offset int64) *seekWriter {
	return &seekWriter{
		File:   f,
		offset: offset,
	}
}

// Write wraps WriteAt function
func (w *seekWriter) Write(p []byte) (n int, err error) {
	n, err = w.WriteAt(p, w.offset)
	if err != nil {
		return
	}
	w.offset += int64(n)
	return
}

// seekReader wrapper for io.ReaderAt
type seekReader struct {
	io.ReaderAt
	offset int64
}

func newSeekReader(r io.ReaderAt, offset int64) *seekReader {
	return &seekReader{
		ReaderAt: r,
		offset:   offset,
	}
}

// Read wraps ReadAt function
func (r *seekReader) Read(p []byte) (n int, err error) {
	n, err = r.ReadAt(p, r.offset)
	if err != nil {
		return
	}
	r.offset += int64(n)
	return
}

// func isLittleEndian() bool {
// 	var i int32 = 0x01020304
// 	return *(*byte)(unsafe.Pointer(&i)) == 0x04
// }
