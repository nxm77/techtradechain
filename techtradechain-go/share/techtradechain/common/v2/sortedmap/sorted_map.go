/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sortedmap

import (
	"sort"
	"sync"
	"sync/atomic"
)

// StringKeySortedMap 以string为key的排序Map
type StringKeySortedMap struct {
	keysLock   sync.RWMutex
	keys       []string
	m          sync.Map
	needResort uint32
}

// NewStringKeySortedMap 构造StringKeySortedMap
// @return *StringKeySortedMap
func NewStringKeySortedMap() *StringKeySortedMap {
	return &StringKeySortedMap{
		keys:       make([]string, 0),
		m:          sync.Map{},
		needResort: 0,
	}
}

// NewStringKeySortedMapWithData 基于map[string]string构造StringKeySortedMap
// @param data
// @return *StringKeySortedMap
func NewStringKeySortedMapWithData(data map[string]string) *StringKeySortedMap {
	if len(data) == 0 {
		return NewStringKeySortedMap()
	}

	sortMap := &StringKeySortedMap{
		keys:       make([]string, 0),
		m:          sync.Map{},
		needResort: 1,
	}

	for key, value := range data {
		sortMap.m.Store(key, value)
		sortMap.keys = append(sortMap.keys, key)
	}

	return sortMap
}

// NewStringKeySortedMapWithBytesData 基于map[string][]byte构造StringKeySortedMap
// @param data
// @return *StringKeySortedMap
func NewStringKeySortedMapWithBytesData(data map[string][]byte) *StringKeySortedMap {
	if len(data) == 0 {
		return NewStringKeySortedMap()
	}

	sortMap := &StringKeySortedMap{
		keys:       make([]string, 0),
		m:          sync.Map{},
		needResort: 1,
	}

	for key, value := range data {
		sortMap.m.Store(key, value)
		sortMap.keys = append(sortMap.keys, key)
	}

	return sortMap
}

// NewStringKeySortedMapWithInterfaceData 基于map[string]interface{}构造StringKeySortedMap
// @param data
// @return *StringKeySortedMap
func NewStringKeySortedMapWithInterfaceData(data map[string]interface{}) *StringKeySortedMap {
	if len(data) == 0 {
		return NewStringKeySortedMap()
	}

	sortMap := &StringKeySortedMap{
		keys:       make([]string, 0),
		m:          sync.Map{},
		needResort: 1,
	}

	for key, value := range data {
		sortMap.m.Store(key, value)
		sortMap.keys = append(sortMap.keys, key)
	}

	return sortMap
}

// Put put KV到Map
// @param key
// @param val
func (m *StringKeySortedMap) Put(key string, val interface{}) {
	_, exist := m.m.LoadOrStore(key, val)
	if exist {
		m.m.Store(key, val)
	} else {
		m.keysLock.Lock()
		m.keys = append(m.keys, key)
		m.keysLock.Unlock()
		atomic.CompareAndSwapUint32(&m.needResort, 0, 1)
	}
}

// Get 根据Key获得Value
// @param key
// @return val
// @return ok
func (m *StringKeySortedMap) Get(key string) (val interface{}, ok bool) {
	return m.m.Load(key)
}

// Contains 是否存在Key
// @param key
// @return ok
func (m *StringKeySortedMap) Contains(key string) (ok bool) {
	_, ok = m.m.Load(key)
	return
}

func (m *StringKeySortedMap) sort() {
	needResort := atomic.LoadUint32(&m.needResort)
	if needResort == 1 {
		m.keysLock.Lock()
		if atomic.LoadUint32(&m.needResort) == 1 {
			sort.Strings(m.keys)
			atomic.StoreUint32(&m.needResort, 0)
		}
		m.keysLock.Unlock()
	}
}

func (m *StringKeySortedMap) removeFromKeys(key string) {
	m.keysLock.Lock()
	if atomic.LoadUint32(&m.needResort) == 1 {
		sort.Strings(m.keys)
		atomic.StoreUint32(&m.needResort, 0)
	}
	length := len(m.keys)
	idx := sort.SearchStrings(m.keys, key)
	if idx != length {
		m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
	}
	m.keysLock.Unlock()
}

// Remove 移除某个Key和对应Value
// @param key
// @return val
// @return ok
func (m *StringKeySortedMap) Remove(key string) (val interface{}, ok bool) {
	val, ok = m.m.LoadAndDelete(key)
	if ok {
		m.removeFromKeys(key)
	}
	return
}

// Range 范围迭代
// @param f
func (m *StringKeySortedMap) Range(f func(key string, val interface{}) (isContinue bool)) {
	m.sort()
	m.keysLock.RLock()
	keys := m.keys
	length := len(keys)
	if length == 0 {
		m.keysLock.RUnlock()
		return
	}
	tempKeys := make([]string, length)
	copy(tempKeys, keys)
	m.keysLock.RUnlock()
	for i := 0; i < length; i++ {
		k := tempKeys[i]
		v, ok := m.m.Load(k)
		if ok && !f(k, v) {
			break
		}
	}
}

// Length 数据量
// @return int
func (m *StringKeySortedMap) Length() int {
	m.keysLock.RLock()
	defer m.keysLock.RUnlock()
	return len(m.keys)
}

// IntKeySortedMap 以int为Key的排序Map
type IntKeySortedMap struct {
	keysLock   sync.RWMutex
	keys       []int
	m          sync.Map
	needResort uint32
}

// NewIntKeySortedMap 构造IntKeySortedMap
// @return *IntKeySortedMap
func NewIntKeySortedMap() *IntKeySortedMap {
	return &IntKeySortedMap{
		keys:       make([]int, 0),
		m:          sync.Map{},
		needResort: 0,
	}
}

// Put 写入KV
// @param key
// @param val
func (m *IntKeySortedMap) Put(key int, val interface{}) {
	_, exist := m.m.LoadOrStore(key, val)
	if exist {
		m.m.Store(key, val)
	} else {
		m.keysLock.Lock()
		m.keys = append(m.keys, key)
		m.keysLock.Unlock()
		atomic.CompareAndSwapUint32(&m.needResort, 0, 1)
	}
}

// Get 查询Key对应的Value
func (m *IntKeySortedMap) Get(key int) (val interface{}, ok bool) {
	return m.m.Load(key)
}

// Contains Key是否存在
// @param key
// @return ok
func (m *IntKeySortedMap) Contains(key int) (ok bool) {
	_, ok = m.m.Load(key)
	return
}

func (m *IntKeySortedMap) sort() {
	needResort := atomic.LoadUint32(&m.needResort)
	if needResort == 1 {
		m.keysLock.Lock()
		if atomic.LoadUint32(&m.needResort) == 1 {
			sort.Ints(m.keys)
			atomic.StoreUint32(&m.needResort, 0)
		}
		m.keysLock.Unlock()
	}
}

func (m *IntKeySortedMap) removeFromKeys(key int) {
	m.keysLock.Lock()
	if atomic.LoadUint32(&m.needResort) == 1 {
		sort.Ints(m.keys)
		atomic.StoreUint32(&m.needResort, 0)
	}
	length := len(m.keys)
	idx := sort.SearchInts(m.keys, key)
	if idx != length {
		m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
	}
	m.keysLock.Unlock()
}

// Remove 移除某个Key和对应Value
// @param key
// @return val
// @return ok
func (m *IntKeySortedMap) Remove(key int) (val interface{}, ok bool) {
	val, ok = m.m.LoadAndDelete(key)
	if ok {
		m.removeFromKeys(key)
	}
	return
}

// Range 范围查询迭代
// @param f
func (m *IntKeySortedMap) Range(f func(val interface{}) (isContinue bool)) {
	m.sort()
	m.keysLock.RLock()
	keys := m.keys
	length := len(keys)
	if length == 0 {
		m.keysLock.RUnlock()
		return
	}
	tempKeys := make([]int, length)
	copy(tempKeys, keys)
	m.keysLock.RUnlock()
	for i := 0; i < length; i++ {
		k := tempKeys[i]
		v, ok := m.m.Load(k)
		if ok && !f(v) {
			break
		}
	}
}

// Length 长度
// @return int
func (m *IntKeySortedMap) Length() int {
	m.keysLock.RLock()
	defer m.keysLock.RUnlock()
	return len(m.keys)
}

// Float64KeySortedMap float64作为Key的排序Map
type Float64KeySortedMap struct {
	keysLock   sync.RWMutex
	keys       []float64
	m          sync.Map
	needResort uint32
}

// NewFloat64KeySortedMap 构造Float64KeySortedMap
// @return *Float64KeySortedMap
func NewFloat64KeySortedMap() *Float64KeySortedMap {
	return &Float64KeySortedMap{
		keys:       make([]float64, 0),
		m:          sync.Map{},
		needResort: 0,
	}
}

// Put 写入KV
// @param key
// @param val
func (m *Float64KeySortedMap) Put(key float64, val interface{}) {
	_, exist := m.m.LoadOrStore(key, val)
	if exist {
		m.m.Store(key, val)
	} else {
		m.keysLock.Lock()
		m.keys = append(m.keys, key)
		m.keysLock.Unlock()
		atomic.CompareAndSwapUint32(&m.needResort, 0, 1)
	}
}

// Get 根据Key查询Value
// @param key
// @return val
// @return ok
func (m *Float64KeySortedMap) Get(key float64) (val interface{}, ok bool) {
	return m.m.Load(key)
}

// Contains 判断Key是否存在
// @param key
// @return ok
func (m *Float64KeySortedMap) Contains(key int) (ok bool) {
	_, ok = m.m.Load(key)
	return
}

func (m *Float64KeySortedMap) sort() {
	needResort := atomic.LoadUint32(&m.needResort)
	if needResort == 1 {
		m.keysLock.Lock()
		if atomic.LoadUint32(&m.needResort) == 1 {
			sort.Float64s(m.keys)
			atomic.StoreUint32(&m.needResort, 0)
		}
		m.keysLock.Unlock()
	}
}

func (m *Float64KeySortedMap) removeFromKeys(key float64) {
	m.keysLock.Lock()
	if atomic.LoadUint32(&m.needResort) == 1 {
		sort.Float64s(m.keys)
		atomic.StoreUint32(&m.needResort, 0)
	}
	length := len(m.keys)
	idx := sort.SearchFloat64s(m.keys, key)
	if idx != length {
		m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
	}
	m.keysLock.Unlock()
}

// Remove 移除Key
// @param key
// @return val
// @return ok
func (m *Float64KeySortedMap) Remove(key float64) (val interface{}, ok bool) {
	val, ok = m.m.LoadAndDelete(key)
	if ok {
		m.removeFromKeys(key)
	}
	return
}

// Range 范围查询迭代
// @param f
func (m *Float64KeySortedMap) Range(f func(key float64, val interface{}) (isContinue bool)) {
	m.sort()
	m.keysLock.RLock()
	keys := m.keys
	length := len(keys)
	if length == 0 {
		m.keysLock.RUnlock()
		return
	}
	tempKeys := make([]float64, length)
	copy(tempKeys, keys)
	m.keysLock.RUnlock()
	for i := 0; i < length; i++ {
		k := tempKeys[i]
		v, ok := m.m.Load(k)
		if ok && !f(k, v) {
			break
		}
	}
}

// Length 长度
// @return int
func (m *Float64KeySortedMap) Length() int {
	m.keysLock.RLock()
	defer m.keysLock.RUnlock()
	return len(m.keys)
}
