/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	"sync"
	"sync/atomic"

	"techtradechain.com/techtradechain/net-liquid/core/network"
	"techtradechain.com/techtradechain/net-liquid/core/peer"
)

// Set .
type Set struct {
	size int32
	m    sync.Map
}

// Put .
// @Description:
// @receiver s
// @param v
// @return bool
func (s *Set) Put(v interface{}) bool {
	_, ok := s.m.LoadOrStore(v, struct{}{})
	if !ok {
		atomic.AddInt32(&s.size, 1)
	}
	return !ok
}

// Remove .
// @Description:
// @receiver s
// @param v
// @return bool
func (s *Set) Remove(v interface{}) bool {
	_, ok := s.m.LoadAndDelete(v)
	if ok {
		atomic.AddInt32(&s.size, -1)
	}
	return ok
}

// Exist .
// @Description:
// @receiver s
// @param v
// @return bool
func (s *Set) Exist(v interface{}) bool {
	if s == nil {
		return false
	}
	_, ok := s.m.Load(v)
	return ok
}

// Range .
// @Description:
// @receiver s
// @param f
func (s *Set) Range(f func(v interface{}) bool) {
	s.m.Range(func(key, _ interface{}) bool {
		return f(key)
	})
}

// Size .
// @Description:
// @receiver s
// @return int
func (s *Set) Size() int {
	return int(atomic.LoadInt32(&s.size))
}

// Uint64Set .
type Uint64Set struct {
	s Set
}

// Put .
// @Description:
// @receiver us
// @param v
// @return bool
func (us *Uint64Set) Put(v uint64) bool {
	return us.s.Put(v)
}

// Remove .
// @Description:
// @receiver us
// @param v
// @return bool
func (us *Uint64Set) Remove(v uint64) bool {
	return us.s.Remove(v)
}

// Exist .
// @Description:
// @receiver us
// @param v
// @return bool
func (us *Uint64Set) Exist(v uint64) bool {
	return us.s.Exist(v)
}

// Size .
// @Description:
// @receiver us
// @return int
func (us *Uint64Set) Size() int {
	return us.s.Size()
}

// Range .
// @Description:
// @receiver us
// @param f
func (us *Uint64Set) Range(f func(v uint64) bool) {
	us.s.Range(func(v interface{}) bool {
		uv, _ := v.(uint64)
		return f(uv)
	})
}

// StringSet .
type StringSet struct {
	s Set
}

// Put .
// @Description:
// @receiver ss
// @param str
// @return bool
func (ss *StringSet) Put(str string) bool {
	return ss.s.Put(str)
}

// Remove .
// @Description:
// @receiver ss
// @param str
// @return bool
func (ss *StringSet) Remove(str string) bool {
	return ss.s.Remove(str)
}

// Exist .
// @Description:
// @receiver ss
// @param str
// @return bool
func (ss *StringSet) Exist(str string) bool {
	return ss.s.Exist(str)
}

// Size .
// @Description:
// @receiver ss
// @return int
func (ss *StringSet) Size() int {
	return ss.s.Size()
}

// Range .
// @Description:
// @receiver ss
// @param f
func (ss *StringSet) Range(f func(str string) bool) {
	ss.s.Range(func(v interface{}) bool {
		uv, _ := v.(string)
		return f(uv)
	})
}

// PeerIdSet .
type PeerIdSet struct {
	s Set
}

// Put .
// @Description:
// @receiver ps
// @param pid
// @return bool
func (ps *PeerIdSet) Put(pid peer.ID) bool {
	return ps.s.Put(pid)
}

// Remove .
// @Description:
// @receiver ps
// @param pid
// @return bool
func (ps *PeerIdSet) Remove(pid peer.ID) bool {
	return ps.s.Remove(pid)
}

// Exist .
// @Description:
// @receiver ps
// @param pid
// @return bool
func (ps *PeerIdSet) Exist(pid peer.ID) bool {
	return ps.s.Exist(pid)
}

// Size .
// @Description:
// @receiver ps
// @return int
func (ps *PeerIdSet) Size() int {
	return ps.s.Size()
}

// Range .
// @Description:
// @receiver ps
// @param f
func (ps *PeerIdSet) Range(f func(pid peer.ID) bool) {
	ps.s.Range(func(v interface{}) bool {
		uv, _ := v.(peer.ID)
		return f(uv)
	})
}

// ConnSet .
type ConnSet struct {
	s Set
}

// Put .
// @Description:
// @receiver cs
// @param conn
// @return bool
func (cs *ConnSet) Put(conn network.Conn) bool {
	return cs.s.Put(conn)
}

// Remove .
// @Description:
// @receiver cs
// @param conn
// @return bool
func (cs *ConnSet) Remove(conn network.Conn) bool {
	return cs.s.Remove(conn)
}

// Exist .
// @Description:
// @receiver cs
// @param conn
// @return bool
func (cs *ConnSet) Exist(conn network.Conn) bool {
	if cs == nil {
		return false
	}
	return cs.s.Exist(conn)
}

// Size .
// @Description:
// @receiver cs
// @return int
func (cs *ConnSet) Size() int {
	return cs.s.Size()
}

// Range .
// @Description:
// @receiver cs
// @param f
func (cs *ConnSet) Range(f func(conn network.Conn) bool) {
	cs.s.Range(func(v interface{}) bool {
		uv, _ := v.(network.Conn)
		return f(uv)
	})
}
