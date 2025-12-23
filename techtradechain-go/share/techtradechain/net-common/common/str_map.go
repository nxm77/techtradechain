/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package common

import "sync"

// StringMapList is a string list using a perfect map
type StringMapList struct {
	lock    sync.Mutex
	mapList map[string]struct{}
}

// NewStringMapList creates a StringMapList
func NewStringMapList() *StringMapList {
	return &StringMapList{
		mapList: make(map[string]struct{}),
	}
}

//Remove StringMapList remove
func (b *StringMapList) Remove(p string) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	//if _, ok := b.mapList[p]; ok {
	delete(b.mapList, p)
	//}
	return true
}

// Add add
func (b *StringMapList) Add(p string) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.mapList[p] = struct{}{}
	return true
}

// Contains .
func (b *StringMapList) Contains(p string) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	_, ok := b.mapList[p]
	return ok
}

// Size .
func (b *StringMapList) Size() int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return len(b.mapList)
}

// List .
func (b *StringMapList) List() []string {
	b.lock.Lock()
	defer b.lock.Unlock()
	l := make([]string, 0)
	for p := range b.mapList {
		l = append(l, p)
	}
	return l
}
