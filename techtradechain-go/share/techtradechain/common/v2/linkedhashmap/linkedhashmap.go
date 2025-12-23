/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package linkedhashmap

import "container/list"

type hashMapNode struct {
	linklistNode *list.Element
	val          interface{}
}

// LinkedHashMap 链接哈希表
type LinkedHashMap struct {
	linklist *list.List
	hashmap  map[string]interface{}
}

// NewLinkedHashMap 新建LinkedHashMap
// @return *LinkedHashMap
func NewLinkedHashMap() *LinkedHashMap {
	return &LinkedHashMap{
		linklist: list.New(),
		hashmap:  make(map[string]interface{}),
	}
}

// Add 添加一个KV到链接哈希表
// @param key
// @param val
// @return bool
func (linkMap *LinkedHashMap) Add(key string, val interface{}) bool {
	if _, isExists := linkMap.hashmap[key]; isExists {
		return false
	}

	linkListNode := linkMap.linklist.PushBack(key)
	linkMap.hashmap[key] = &hashMapNode{
		linklistNode: linkListNode,
		val:          val,
	}
	return true
}

// Get 根据Key获得Value
// @param key
// @return interface{}
func (linkMap *LinkedHashMap) Get(key string) interface{} {
	originLinkedHashMapNode, isExists := linkMap.hashmap[key]
	if !isExists {
		return nil
	}
	return (originLinkedHashMapNode.(*hashMapNode)).val //nolint:errcheck
}

// Size 占用大小
// @return int
func (linkMap *LinkedHashMap) Size() int {
	return len(linkMap.hashmap)
}

// Remove 移除指定Key的KV
// @param key
// @return bool 是否移除成功
// @return interface{} 被移除的对象Value
func (linkMap *LinkedHashMap) Remove(key string) (bool, interface{}) {
	originLinkedHashMapNode, isExists := linkMap.hashmap[key]
	if !isExists {
		return false, nil
	}

	linkedHashMapNode, _ := originLinkedHashMapNode.(*hashMapNode)
	delete(linkMap.hashmap, key)
	linkMap.linklist.Remove(linkedHashMapNode.linklistNode)
	return true, linkedHashMapNode.val
}

// GetLinkList 以list结构返回
// @return *list.List
func (linkMap *LinkedHashMap) GetLinkList() *list.List {
	return linkMap.linklist
}
