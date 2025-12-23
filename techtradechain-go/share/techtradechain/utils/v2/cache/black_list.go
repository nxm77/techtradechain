/*
 * Copyright (C) BABEC. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
/*
 * sample cache with sync.map
 */

package cache

import (
	"sync"
)

var (
	lock          = &sync.Mutex{}
	cacheInstance = make(map[string]*CacheList)
)

// CacheList sample map
type CacheList struct {
	sync.RWMutex
	//elements sync.Map
	// modify elements to map[string]struct{}, because we need get length
	elements map[string]struct{}
}

// NewCacheList get CacheList instance
func NewCacheList(name string) *CacheList {
	instance, ok := cacheInstance[name]
	if !ok {
		lock.Lock()
		defer lock.Unlock()
		instance, ok = cacheInstance[name]
		if !ok {
			instance = &CacheList{elements: make(map[string]struct{})}
			cacheInstance[name] = instance
		}
	}
	return instance
}

// Put insert the key to map
func (b *CacheList) Put(key string) {
	b.Lock()
	defer b.Unlock()
	b.elements[key] = struct{}{}
}

// Delete del key
func (b *CacheList) Delete(key string) {
	b.Lock()
	defer b.Unlock()
	delete(b.elements, key)
}

// Exists exists key,notfound return false
func (b *CacheList) Exists(key string) bool {
	b.RLock()
	defer b.RUnlock()
	_, ok := b.elements[key]
	return ok
}

// Len get length
func (b *CacheList) Len() int {
	b.RLock()
	defer b.RUnlock()
	return len(b.elements)
}

// Empty return bool is empty
func (b *CacheList) Empty() bool {
	return b.Len() == 0
}
