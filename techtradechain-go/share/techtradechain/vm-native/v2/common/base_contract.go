/*
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import "sort"

// BaseContract 所有Native合约的基础合约，提供了方法注册和获取
type BaseContract struct {
	methods map[string][]methodInfo
}
type methodInfo struct {
	methodName   string
	method       ContractFunc
	startVersion uint32
	endVersion   uint32
}

// isActive 是否活跃，检测方法在指定的版本是否生效前闭后开[startVersion, endVersion)
func (m methodInfo) isActive(version uint32) bool {
	if m.startVersion > 0 && version < m.startVersion {
		return false
	}
	if m.endVersion > 0 && version >= m.endVersion {
		return false
	}
	return true
}

// GetMethod 根据合约方法名和版本获得对应的合约函数
func (g *BaseContract) GetMethod(methodName string, blockVersion uint32) ContractFunc {
	methods := g.methods[methodName]
	for _, m := range methods {
		if m.isActive(blockVersion) {
			return m.method
		}
	}
	return nil
}

// RegisterMethod 注册一个新的合约方法，指定方法的生效区间，前闭后开，0代表不指定版本.
func (g *BaseContract) RegisterMethod(methodName string, f ContractFunc, startVersion uint32, endVersion uint32) {
	m := methodInfo{
		methodName:   methodName,
		method:       f,
		startVersion: startVersion,
		endVersion:   endVersion,
	}
	if g.methods == nil {
		g.methods = make(map[string][]methodInfo)
	}
	existMethod, ok := g.methods[methodName]
	if !ok {
		existMethod = []methodInfo{}
	}
	//如果有多个version，那么以startVersion大的排前面，这样如果有区间重合的时候，先拿新版本的。
	existMethod = append(existMethod, m)
	sort.Sort(methodInfoSliceDecrement(existMethod))
	g.methods[methodName] = existMethod

}

type methodInfoSliceDecrement []methodInfo

func (s methodInfoSliceDecrement) Len() int { return len(s) }

func (s methodInfoSliceDecrement) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s methodInfoSliceDecrement) Less(i, j int) bool { return s[i].startVersion > s[j].startVersion }

// InitMethod 初始化一堆没有版本限制的方法
// @param ms
func (g *BaseContract) InitMethod(ms map[string]ContractFunc) {
	g.methods = make(map[string][]methodInfo)
	for name, method := range ms {
		g.RegisterMethod(name, method, 0, 0)
	}
}
