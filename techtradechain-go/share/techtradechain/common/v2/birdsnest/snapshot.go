/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package birdsnest snapshot
package birdsnest

// nolint:staticcheck
import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"techtradechain.com/techtradechain/common/v2/wal"
)

// WalSnapshot wal snapshot
type WalSnapshot struct {
	snapshotM sync.Mutex
	wal       *wal.Log
}

// NewWalSnapshot new wal snapshot eg: data/tx_filter/chainN/birdsnestN
func NewWalSnapshot(path, name string, number int) (*WalSnapshot, error) {
	opts := wal.DefaultOptions
	opts.NoSync = false
	if number > 0 {
		// eg: data/txfilter/chainN/birdnest1
		path = filepath.Join(path, name+strconv.Itoa(number))
	} else {
		// eg: data/txfilter/chainN/birdnest
		path = filepath.Join(path, name)
	}
	err := createDirIfNotExist(path)
	if err != nil {
		return nil, err
	}
	file, err := wal.Open(path, opts)
	if err != nil {
		return nil, err
	}
	ws := &WalSnapshot{
		wal:       file,
		snapshotM: sync.Mutex{},
	}
	return ws, nil
}

// Read safe read wal
func (s *WalSnapshot) Read() ([]byte, error) {
	s.snapshotM.Lock()
	defer s.snapshotM.Unlock()
	index, err := s.wal.LastIndex()
	if err != nil {
		return nil, err
	}
	if index == 0 {
		return nil, nil
	}
	read, err := s.wal.Read(index)
	if err != nil {
		return nil, err
	}
	return read, nil
}

// Write safe write wal
func (s *WalSnapshot) Write(data []byte) error {
	s.snapshotM.Lock()
	defer s.snapshotM.Unlock()
	index, err := s.wal.LastIndex()
	if err != nil {
		return err
	}
	index++
	err = s.wal.Write(index, data)
	if err != nil {
		return err
	}
	err = s.wal.TruncateFront(index)
	if err != nil {
		return err
	}
	return nil
}

// createDirIfNotExist create dir if not exist
func createDirIfNotExist(path string) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		// create dir
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

//type LwsSnapshot struct {
//	snapshotM sync.Mutex
//	lws       *lws.Lws
//}
//
//func NewLwsSnapshot(path, name string, number int) (*LwsSnapshot, error) {
//	if number > 0 {
//		path = filepath.Join(path, name+strconv.Itoa(number))
//	} else {
//		path = filepath.Join(path, name)
//	}
//	l, err := lws.Open(path,
//		lws.WithWriteFlag(lws.WF_TIMEDFLUSH, 1000),
//		lws.WithEntryLimitForPurge(2),
//		//lws.WithWriteFlag()
//	)
//	if err != nil {
//		return nil, err
//	}
//	//l.
//	err = createDirIfNotExist(path)
//	if err != nil {
//		return nil, err
//	}
//	ws := &LwsSnapshot{
//		lws:       l,
//		snapshotM: sync.Mutex{},
//	}
//	return ws, nil
//}
//
//func (s *LwsSnapshot) Read() ([]byte, error) {
//
//	return nil, nil
//}
//
//func (s *LwsSnapshot) Write(data []byte) error {
//	return nil
//}

const (
	snapFilePrefix  = "bird_filter_"
	snapCfgPrefix   = "bird_cfg"
	snapGaugePrefix = "bird_gauge"
)

// fileSnapshot  snapshot
type filterSnapshot struct {
	snapshotM sync.Mutex
	logPath   string
}

// newWalSnapshot new wal snapshot eg: data/tx_filter/chainN/birdsnestN
func newFilterSnapshot(path, name string, number int) (*filterSnapshot, error) {
	if number > 0 {
		// eg: data/txfilter/chainN/birdnest1
		path = filepath.Join(path, name+strconv.Itoa(number))
	} else {
		// eg: data/txfilter/chainN/birdnest
		path = filepath.Join(path, name)
	}
	err := createDirIfNotExist(path)
	if err != nil {
		return nil, err
	}

	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	ws := &filterSnapshot{
		logPath:   path,
		snapshotM: sync.Mutex{},
	}
	return ws, nil
}

// Read cfg
func (s *filterSnapshot) ReadCfg() ([]byte, error) {
	s.snapshotM.Lock()
	defer s.snapshotM.Unlock()

	path := s.logPath
	// read cfg
	cfgFileName := filepath.Join(path, snapCfgPrefix)
	data, err := ioutil.ReadFile(cfgFileName)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *filterSnapshot) Read(height int) ([][]byte, error) {
	s.snapshotM.Lock()
	defer s.snapshotM.Unlock()

	var res [][]byte
	// read height index
	path := s.logPath
	gFileName := filepath.Join(path, snapGaugePrefix)
	data, err := ioutil.ReadFile(gFileName)
	if err != nil {
		return nil, err
	}
	res = append(res, data)

	// read filter
	for i := 0; i <= height; i++ {
		fileName := filepath.Join(path, snapFilePrefix+fmt.Sprintf("%05d", i))
		data, err = ioutil.ReadFile(fileName)
		if err != nil {
			return nil, err
		}
		res = append(res, data)
	}

	return res, nil
}

// Write cfg
func (s *filterSnapshot) WriteCfg(data []byte) error {
	s.snapshotM.Lock()
	defer s.snapshotM.Unlock()
	//path, _ := filepath.Abs(s.logPath)
	cfgFileName := filepath.Join(s.logPath, snapCfgPrefix)
	return writeFile(cfgFileName, data)
}

// Write Gauge
func (s *filterSnapshot) WriteGauge(data []byte) error {
	s.snapshotM.Lock()
	defer s.snapshotM.Unlock()
	cfgFileName := filepath.Join(s.logPath, snapGaugePrefix)
	return writeFile(cfgFileName, data)
}

// Write Filter
func (s *filterSnapshot) WriteFilter(data []byte, index uint16) error {
	s.snapshotM.Lock()
	defer s.snapshotM.Unlock()
	fileName := filepath.Join(s.logPath, snapFilePrefix+fmt.Sprintf("%05d", index))
	return writeFile(fileName, data)
}

// writeFile
func writeFile(fileName string, data []byte) error {
	tmpFileName := fileName + ".tmp"
	_ = os.Remove(tmpFileName)
	pTmpLog, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer pTmpLog.Close()

	_, err = pTmpLog.Write(data)
	if err != nil {
		return err
	}
	_ = pTmpLog.Close()
	_ = os.Remove(fileName)
	return os.Rename(tmpFileName, fileName)
}
