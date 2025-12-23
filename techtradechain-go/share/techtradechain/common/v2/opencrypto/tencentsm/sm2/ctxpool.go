/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sm2

import "C"
import (
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"sync"

	"techtradechain.com/techtradechain/common/v2/opencrypto/tencentsm/tencentsm"
	lru "github.com/hashicorp/golang-lru"
	"github.com/spf13/viper"
)

const (
	// CTX_POOLSIZE_PER_PK is the default ctx pool size for each pk
	CTX_POOLSIZE_PER_PK = 10
	CTXPOOL_CACHESIZE   = 10000
)

var (
	lock            sync.RWMutex
	pubCtxPoolCache *lru.Cache
)

func init() {
	var err error
	pubCtxPoolCache, err = lru.New(CTXPOOL_CACHESIZE)
	if err != nil {
		panic("tencentsm public ctxpool init failed")
	}
}

// CtxPool is tencentsm public ctx pool
type CtxPool struct {
	poolSize int
	ctxChan  chan *tencentsm.SM2_ctx_t
	pubkey   []byte

	lock sync.Mutex
}

// NewCtxPoolWithPubKey new a ctx pool for pubkey
func NewCtxPoolWithPubKey(pubkey []byte) *CtxPool {
	//if exist
	dgst := sha256.Sum256(pubkey)
	pubHex := hex.EncodeToString(dgst[:20])
	lock.Lock()
	defer lock.Unlock()
	if val, ok := pubCtxPoolCache.Get(pubHex); ok {
		return val.(*CtxPool)
	}

	//new pool per public key
	poolSize := CTX_POOLSIZE_PER_PK
	if viper.IsSet("common.tencentsm.ctx_pool_size") {
		ctxPoolSize := viper.GetInt("common.tencentsm.ctx_pool_size")
		if ctxPoolSize > 0 {
			poolSize = ctxPoolSize
		}
	}

	//check again
	if val, ok := pubCtxPoolCache.Get(pubHex); ok {
		return val.(*CtxPool)
	}

	pool := &CtxPool{
		ctxChan:  make(chan *tencentsm.SM2_ctx_t, poolSize),
		poolSize: poolSize,
		pubkey:   pubkey,
	}

	//init tencentsm sm2Ctx
	go func() {
		for j := 0; j < poolSize; j++ {
			var ctx tencentsm.SM2_ctx_t
			tencentsm.SM2InitCtxWithPubKey(&ctx, pubkey)
			pool.ctxChan <- &ctx
			runtime.Gosched()
		}
	}()
	pubCtxPoolCache.Add(pubHex, pool)
	return pool
}

// GetCtx get a ctx from pool
func (c *CtxPool) GetCtx() *tencentsm.SM2_ctx_t {
	select {
	case sm2Ctx := <-c.ctxChan:
		return sm2Ctx
	default:
		var ctx tencentsm.SM2_ctx_t
		tencentsm.SM2InitCtxWithPubKey(&ctx, c.pubkey)
		return &ctx
	}
}

// ReleaseCtx release a ctx to ctx pool
func (c *CtxPool) ReleaseCtx(ctx *tencentsm.SM2_ctx_t) {
	select {
	case c.ctxChan <- ctx:
	default:
		tencentsm.SM2FreeCtx(ctx)
	}
}

// Close close the ctx pool, free all ctx
func (c *CtxPool) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	close(c.ctxChan)

	for ctx := range c.ctxChan {
		tencentsm.SM2FreeCtx(ctx)
	}
}
