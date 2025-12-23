/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package shardingbirdsnest serialize
package shardingbirdsnest

import (
	"encoding/json"
	"reflect"
	"time"

	bn "techtradechain.com/techtradechain/common/v2/birdsnest"
	"techtradechain.com/techtradechain/common/v2/shardingbirdsnest/pb"
	"github.com/gogo/protobuf/proto"
)

// Start TODO Goroutinue should be turned off using context.Context here
func (s *ShardingBirdsNest) Start() {
	// start serialize monitor
	go s.serializeMonitor()
	// start serialize timed
	go s.serializeTimed()
	// start all bird's nest
	for i := range s.bn {
		s.bn[i].Start()
	}
}

// Serialize serialize to wal
func (s *ShardingBirdsNest) Serialize() error {
	t := time.Now()
	// Logs are print after the method is complete
	defer func(log bn.Logger) {
		elapsed := time.Since(t)
		log.Infof("sharding bird's nest serialize success elapsed: %v", elapsed)
	}(s.log)

	// config to json
	conf, err := json.Marshal(s.config)
	if err != nil {
		return err
	}
	// sharding bird's nest pb
	sbn := &pb.ShardingBirdsNest{
		Length: s.config.Length,
		Height: s.height,
		Config: conf,
	}
	// sharding bird's nest pb to byte
	data, err := proto.Marshal(sbn)
	if err != nil {
		return err
	}
	// snapshot data
	err = s.snapshot.Write(data)
	if err != nil {
		return err
	}
	// cover pre height
	s.preHeight.Store(s.height)
	return nil
}

// Deserialize deserialize configuration
func (s *ShardingBirdsNest) Deserialize() error {
	// read snapshot
	data, err := s.snapshot.Read()
	if err != nil {
		return err
	}
	// If there is no data on the disk, the procedure ends
	if data == nil {
		return nil
	}
	sharding := new(pb.ShardingBirdsNest)
	// unmarshal data to pb sharding bird's nest
	err = proto.Unmarshal(data, sharding)
	if err != nil {
		return err
	}
	var sbnConfig ShardingBirdsNestConfig
	err = json.Unmarshal(sharding.Config, &sbnConfig)
	if err != nil {
		return err
	}
	// equal disk config and file config
	if reflect.DeepEqual(sbnConfig, s.config) {
		err = ErrCannotModifyTheNestConfiguration
	}
	s.height = sharding.Height
	return err
}

// serializeMonitor start serialize monitor TODO Goroutinue should be turned off using context.Context here
func (s *ShardingBirdsNest) serializeMonitor() {
	for { // nolint
		select {
		// 只有当前"序列化类型"的信号才能过来
		case signal := <-s.serializeC:
			t, ok := bn.SerializeIntervalType_name[signal.typ]
			if !ok {
				s.log.Errorf("serialize type %v not support", t)
			}
			switch signal.typ {
			case bn.SerializeIntervalType_Height:
				// 并且 当前高度 - 上次持久化高度 < 高度间隔 则不做持久化 否则，执行持久化
				// eg: 85 - 80 = 5 < 10
				// 	   5 < 10 true 则不做持久化
				if s.height-s.preHeight.Load() < s.config.Snapshot.BlockHeight.Interval {
					continue
				}
			case bn.SerializeIntervalType_Timed, bn.SerializeIntervalType_Exit:
				// "时间序列化类型"和"退出序列化类型"直接处理
			default:
				continue
			}
			// serialize
			err := s.Serialize()
			if err != nil {
				s.log.Errorf("serialize error type: %v, error: %v", t, err)
			}
		}
	}
}

// serializeTimed serialize by timed sign
func (s *ShardingBirdsNest) serializeTimed() {
	// The current serialization type terminates if it is not bn.SerializeIntervalType_Timed
	if s.config.Snapshot.Type != bn.SerializeIntervalType_Timed {
		return
	}
	// start Timing task
	ticker := time.NewTicker(time.Second * time.Duration(s.config.Snapshot.Timed.Interval))
	// nolint
	for {
		select {
		case <-ticker.C:
			// Timing signal
			s.serializeC <- serializeSignal{typ: bn.SerializeIntervalType_Timed}
		}
	}
}

// serializeExit serialize by exit sign
// nolint: unused
func (s *ShardingBirdsNest) serializeExit() {
	// send bn.SerializeIntervalType_Exit sign
	s.serializeC <- serializeSignal{typ: bn.SerializeIntervalType_Exit}
}

// serializeHeight serialize by SerializeIntervalType_Height sign
func (s *ShardingBirdsNest) serializeHeight(height uint64) {
	if s.config.Snapshot.Type != bn.SerializeIntervalType_Height {
		return
	}
	// send bn.SerializeIntervalType_Height sign
	s.serializeC <- serializeSignal{typ: bn.SerializeIntervalType_Height, height: height}
}

// Serialize signal
type serializeSignal struct {
	// typ bn.SerializeIntervalType type
	typ bn.SerializeIntervalType
	// current height
	height uint64
}
