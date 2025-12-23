/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package lws

type (
	FileType  int
	WriteFlag int
	purgeMod  int
)

const (
	WF_SYNCWRITE WriteFlag = 1 << iota //同步写，写系统不刷盘
	// WF_TIMEDFLUSH WriteFlag = (1<<iota - 1) << 1 //定时刷盘
	WF_TIMEDFLUSH //定时刷盘
	WF_QUOTAFLUSH //日志写入数量刷盘
	WF_SYNCFLUSH  //同步刷盘
)

const (
	FT_NORMAL FileType = iota
	FT_MMAP
)

const (
	purgeModSync  purgeMod = 0
	purgeModAsync purgeMod = 1
)

type Options struct {
	Wf                         WriteFlag //写日志标识
	FlushQuota                 int       //刷盘限定值
	SegmentSize                int64     //文件的大小限制 默认64M 代表不限制
	Ft                         FileType  //文件类型(1 普通文件 2 mmap) 默认1
	MmapFileLock               bool      //文件映射的时候，是否锁定内存以提高write速度
	BufferSize                 int
	LogFileLimitForPurge       int //存在日志文件限制
	LogEntryCountLimitForPurge int //存在日志条目限制
	FilePrefix                 string
	FileExtension              string
	//CopyForRead 从lws底层读取数据时是否copy，不进行copy意味着上层获取到的数据指向了lws缓存层
	//lws再次进行读写操作时可能导致缓存层失效，所以使用数据时请确保不对lws再次进行读写操作
	CopyForRead bool
}

type Opt func(*Options)

func WithWriteFlag(wf WriteFlag, quota int) Opt {
	return func(o *Options) {
		o.Wf = wf
		o.FlushQuota = quota
	}
}

func WithSegmentSize(s int64) Opt {
	return func(o *Options) {
		o.SegmentSize = s
	}
}

func WithWriteFileType(ft FileType) Opt {
	return func(o *Options) {
		o.Ft = ft
	}
}

func WithFileLimitForPurge(l int) Opt {
	return func(o *Options) {
		o.LogFileLimitForPurge = l
	}
}

func WithEntryLimitForPurge(l int) Opt {
	return func(o *Options) {
		o.LogEntryCountLimitForPurge = l
	}
}

func WithFilePrex(prex string) Opt {
	return func(o *Options) {
		o.FilePrefix = prex
	}
}

func WithFileExtension(ext string) Opt {
	return func(o *Options) {
		o.FileExtension = ext
	}
}

func WithMmapFileLock() Opt {
	return func(o *Options) {
		o.MmapFileLock = true
	}
}

//设置lws缓存层大小
func WithBufferSize(s int) Opt {
	return func(o *Options) {
		o.BufferSize = s
	}
}

//读取数据lws不自行进行copy
func WithReadNoCopy() Opt {
	return func(o *Options) {
		o.CopyForRead = false
	}
}

type PurgeOptions struct {
	mode purgeMod
	purgeLimit
}
type purgeLimit struct {
	keepFiles int
	// keepEntries     int
	keepSoftEntries int
}

type PurgeOpt func(*PurgeOptions)

func PurgeWithKeepFiles(c int) PurgeOpt {
	return func(po *PurgeOptions) {
		po.keepFiles = c
	}
}

func PurgeWithSoftEntries(c int) PurgeOpt {
	return func(po *PurgeOptions) {
		po.keepSoftEntries = c
	}
}

func PurgeWithAsync() PurgeOpt {
	return func(po *PurgeOptions) {
		po.mode = purgeModAsync
	}
}

func (wf WriteFlag) onlyHas(in WriteFlag) bool {
	return wf == in
}

func (wf WriteFlag) has(in WriteFlag) bool {
	return wf&in == in
}

func (wf WriteFlag) exclude(in WriteFlag) WriteFlag {
	return wf &^ in
}
