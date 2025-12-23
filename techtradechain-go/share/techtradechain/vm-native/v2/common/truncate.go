/*
 * Copyright (C) BABEC. All rights reserved.
 * Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"crypto/sha256"
	"strings"

	commonPb "techtradechain.com/techtradechain/pb-go/v2/common"
)

// TruncateConfig util to truncate struct field size
type TruncateConfig struct {
	truncateValueLen int
	truncateModel    string
	truncateFunc     func([]byte) []byte
}

// NewTruncateConfig 创建一个裁剪操作对象
// @param truncateValueLen
// @param truncateModel
// @return *TruncateConfig
func NewTruncateConfig(truncateValueLen int, truncateModel string) *TruncateConfig {
	cfg := &TruncateConfig{truncateValueLen: truncateValueLen, truncateModel: truncateModel}
	switch strings.ToLower(truncateModel) {
	case "hash":
		cfg.truncateFunc = func(i []byte) []byte {
			hash := sha256.Sum256(i)
			return hash[:]
		}
	case "truncate":
		cfg.truncateFunc = func(i []byte) []byte {
			return i[:truncateValueLen]
		}
	case "empty":
		cfg.truncateFunc = func(_ []byte) []byte {
			return []byte{}
		}
	default:
		cfg.truncateFunc = func(_ []byte) []byte {
			return []byte(cfg.truncateModel)
		}
	}
	return cfg
}

// TruncatePayload 裁剪交易
// @param payload
func (t *TruncateConfig) TruncatePayload(payload *commonPb.Payload) {
	if t.truncateValueLen == 0 {
		return
	}

	for _, p := range payload.Parameters {
		if len(p.Value) <= t.truncateValueLen {
			continue
		}
		p.Value = t.truncateFunc(p.Value)
	}
}

// TruncateTx 裁剪交易
// @param tx
func (t *TruncateConfig) TruncateTx(tx *commonPb.Transaction) {
	t.TruncatePayload(tx.Payload)
}

// TruncateRWSet 裁剪读写集
// @param rwset
func (t *TruncateConfig) TruncateRWSet(rwset *commonPb.TxRWSet) {
	if t.truncateValueLen == 0 {
		return
	}
	if rwset == nil {
		return
	}
	for _, r := range rwset.TxReads {
		if len(r.Value) <= t.truncateValueLen {
			continue
		}
		r.Value = t.truncateFunc(r.Value)
	}
	for _, w := range rwset.TxWrites {
		if len(w.Value) <= t.truncateValueLen {
			continue
		}
		w.Value = t.truncateFunc(w.Value)
	}
}

// TruncateBlock 裁剪区块
// @param b
func (t *TruncateConfig) TruncateBlock(b *commonPb.Block) {
	for _, tx := range b.Txs {
		t.TruncateTx(tx)
	}
}

// TruncateBlockWithRWSet 裁剪区块和读写集
// @param b
func (t *TruncateConfig) TruncateBlockWithRWSet(b *commonPb.BlockInfo) {
	t.TruncateBlock(b.Block)
	for _, rwset := range b.RwsetList {
		t.TruncateRWSet(rwset)
	}
}
