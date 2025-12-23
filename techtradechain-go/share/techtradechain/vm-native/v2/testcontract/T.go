/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Package testcontract is package for testcontract
package testcontract

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"

	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/vm-native/v2/common"
)

const (
	topicPrefix     = "topic_" // prefix of topic
	eventPrefix     = "event_" // prefix of event data
	contractVersion = "v1"     // version in T contract
)

var (
	//ContractName current contract name
	ContractName = syscontract.SystemContract_T.String()
)

// Manager contract manager
type Manager struct {
	methods map[string]common.ContractFunc
	log     protocol.Logger
}

// NewManager constructor of Manager
// @param log
// @return *Manager
func NewManager(log protocol.Logger) *Manager {
	return &Manager{
		log:     log,
		methods: registerTestContractMethods(log),
	}
}

// GetMethod get register method by name
func (c *Manager) GetMethod(methodName string) common.ContractFunc {
	return c.methods[methodName]
}

func registerTestContractMethods(log protocol.Logger) map[string]common.ContractFunc {
	methodMap := make(map[string]common.ContractFunc, 64)
	runtime := &ManagerRuntime{log: log}
	methodMap[syscontract.TestContractFunction_P.String()] = common.WrapResultFunc(runtime.put)
	methodMap[syscontract.TestContractFunction_G.String()] = common.WrapResultFunc(runtime.get)
	methodMap[syscontract.TestContractFunction_D.String()] = common.WrapResultFunc(runtime.del)
	methodMap[syscontract.TestContractFunction_N.String()] = common.WrapResultFunc(runtime.nothing)
	//以下是各种性能测试用的查询
	methodMap["Q"] = common.WrapResultFunc(runtime.queryTest)
	methodMap["R"] = common.WrapResultFunc(runtime.rangeTest)
	methodMap["H"] = common.WrapResultFunc(runtime.historyTest)
	// 以下为新增用于产生事件的合约
	methodMap["E"] = common.WrapEventResult(runtime.emitEvents)

	return methodMap
}

// ManagerRuntime runtime instance
type ManagerRuntime struct {
	log protocol.Logger
}

// get get state by key["k"]
func (r *ManagerRuntime) get(context protocol.TxSimContext, parameters map[string][]byte) ([]byte, error) {
	return context.Get(ContractName, parameters["k"])
}

// put put state by key="k" value="v"
func (r *ManagerRuntime) put(context protocol.TxSimContext, parameters map[string][]byte) ([]byte, error) {
	k := parameters["k"]
	v := parameters["v"]
	return nil, context.Put(ContractName, k, v)
}

// del delete state by key="k"
func (r *ManagerRuntime) del(context protocol.TxSimContext, parameters map[string][]byte) ([]byte, error) {
	return nil, context.Del(ContractName, parameters["k"])
}

// nothing  do nothing
func (r *ManagerRuntime) nothing(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	return nil, nil
}

// queryTest Query Test,这是一个大量GetState的性能测试方法，主要是为了测试在一个合约方法中如果进行了大量的GetState，会耗费多少的时间
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *ManagerRuntime) queryTest(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	cname := string(parameters["contract"])
	start, _ := strconv.Atoi(string(parameters["start"]))
	end, _ := strconv.Atoi(string(parameters["end"]))
	key := string(parameters["key"])
	count := 0
	startTime := time.Now()
	for i := start; i < end; i++ {
		stateKey := fmt.Sprintf(key, i)
		v, err := txSimContext.Get(cname, []byte(stateKey))
		if err != nil {
			return nil, fmt.Errorf("query contract[%s] by key[%s] get error: %s", cname, stateKey, err)
		}
		if len(v) > 0 {
			count++
		}
	}
	return []byte(fmt.Sprintf("query count=%d, spend time:%v", count, time.Since(startTime))), nil
}

// rangeTest 范围查询（前缀查询）性能测试用
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *ManagerRuntime) rangeTest(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	cname := string(parameters["contract"])
	count, _ := strconv.Atoi(string(parameters["count"]))
	prefix := parameters["prefix"]
	resultType := string(parameters["result"])
	start, limit := bytesPrefix(prefix)
	startTime := time.Now()
	iter, err := txSimContext.Select(cname, start, limit)
	if err != nil {
		return nil, fmt.Errorf("query contract[%s] by prefix[%s] get error: %s", cname, string(prefix), err)
	}
	defer iter.Release()
	i := 0
	result := ""
	for iter.Next() {
		kv, _ := iter.Value()
		if resultType == "key" {
			result += string(kv.Key) + ";"
		} else if resultType == "kv" {
			result += kv.String() + ";"
		}
		i++
		if i >= count {
			break
		}
	}
	return []byte(fmt.Sprintf("range query keys=%s, spend time:%v", result, time.Since(startTime))), nil
}
func bytesPrefix(prefix []byte) ([]byte, []byte) {
	var limit []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			limit = make([]byte, i+1)
			copy(limit, prefix)
			limit[i] = c + 1
			break
		}
	}
	return prefix, limit
}

// historyTest Key的历史记录查询
// @param txSimContext
// @param parameters
// @return []byte
// @return error
func (r *ManagerRuntime) historyTest(txSimContext protocol.TxSimContext, parameters map[string][]byte) (
	[]byte, error) {
	cname := string(parameters["contract"])
	count, _ := strconv.Atoi(string(parameters["count"]))
	key := parameters["key"]
	resultType := string(parameters["result"])

	startTime := time.Now()
	iter, err := txSimContext.GetHistoryIterForKey(cname, key)
	if err != nil {
		return nil, fmt.Errorf("query contract[%s] by key[%s] get error: %s", cname, string(key), err)
	}
	defer iter.Release()
	i := 0
	result := ""
	for iter.Next() {
		kv, _ := iter.Value()
		if resultType == "txid" {
			result += string(kv.TxId) + ";"
		} else if resultType == "all" {
			result += kv.String() + ";"
		}
		i++
		if i >= count {
			break
		}
	}
	return []byte(fmt.Sprintf("history query result=%s, spend time:%v", result, time.Since(startTime))), nil
}

// emitEvents 发布一堆事件，用于事件测试
// @param events 事件列表，格式："topic_0":"event_0";"topic_1":"event_1"
// 如果需要一个topic对应多个事件，则事件的内容中使用","进行分割
func (r *ManagerRuntime) emitEvents(txSimContext protocol.TxSimContext, params map[string][]byte) (
	[]byte, []*commonPb.ContractEvent, error) {
	// 初始化两个map来分别存储topic和event数据
	topics, events := make(map[string]string), make(map[string]string)

	// 遍历params，分类存储topic和event数据
	for key, value := range params {
		if strings.HasPrefix(key, topicPrefix) {
			topics[key] = string(value)
		} else if strings.HasPrefix(key, eventPrefix) {
			events[key] = string(value)
		}
	}
	if len(topics) == 0 && len(events) == 0 {
		return []byte("ok"), nil, nil
	}
	// 获取所有数字后缀
	var indices []int
	for key := range topics {
		idxStr := strings.TrimPrefix(key, topicPrefix)
		idx, err := strconv.Atoi(idxStr)
		if err == nil {
			indices = append(indices, idx)
		}
	}
	for key := range events {
		idxStr := strings.TrimPrefix(key, eventPrefix)
		idx, err := strconv.Atoi(idxStr)
		if err == nil && !contains(indices, idx) {
			indices = append(indices, idx)
		}
	}
	if len(indices) == 0 {
		return []byte("ok"), nil, nil
	}
	// 对索引进行排序
	sort.Ints(indices)
	retEvents := make([]*commonPb.ContractEvent, 0)
	// 按照排序后的索引配对打印
	for _, idx := range indices {
		topicKey := fmt.Sprintf(topicPrefix+"%d", idx)
		eventKey := fmt.Sprintf(eventPrefix+"%d", idx)
		topicValue, topicExists := topics[topicKey]
		eventValue, eventExists := events[eventKey]
		if !topicExists {
			topicValue = ""
		}
		if !eventExists {
			eventValue = ""
		}
		retEvent := &commonPb.ContractEvent{
			Topic:           topicValue,
			TxId:            txSimContext.GetTx().Payload.TxId,
			ContractName:    ContractName,
			ContractVersion: contractVersion,
		}
		if len(eventValue) > 0 {
			eventValues := strings.Split(eventValue, ",")
			retEvent.EventData = eventValues
		} else {
			retEvent.EventData = []string{eventValue}
		}
		retEvents = append(retEvents, retEvent)
	}
	return []byte("ok"), retEvents, nil
}

// contains 辅助函数：检查slice是否包含某个元素
func contains(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
