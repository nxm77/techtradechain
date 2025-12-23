/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dpos

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"

	"techtradechain.com/techtradechain/pb-go/v2/common"
	configPb "techtradechain.com/techtradechain/pb-go/v2/config"
	"techtradechain.com/techtradechain/pb-go/v2/consensus"
	"techtradechain.com/techtradechain/pb-go/v2/store"
	"techtradechain.com/techtradechain/pb-go/v2/syscontract"
	"techtradechain.com/techtradechain/protocol/v2"
	"techtradechain.com/techtradechain/protocol/v2/mock"
	"techtradechain.com/techtradechain/protocol/v2/test"
	native "techtradechain.com/techtradechain/vm-native/v2/dposmgr"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
)

var (
	testAddr        = "addr1-balance"
	testAddrBalance = 9999
	errContract     = "should_error"
	testEpochBlkNum = 4
)

//  mock function
func newMockBlockChainStore(ctrl *gomock.Controller) *mock.MockBlockchainStore {
	mockStore := baseMockStore(ctrl)
	iter := mock.NewMockStateIterator(ctrl)
	iter.EXPECT().Release().AnyTimes()
	iter.EXPECT().Value().AnyTimes()
	iter.EXPECT().Next().AnyTimes()
	mockStore.EXPECT().SelectObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(contractName string, startKey []byte, limit []byte) (protocol.StateIterator, error) {
			return iter, nil
		}).AnyTimes()
	return mockStore
}

//  mock function
func baseMockStore(ctrl *gomock.Controller) *mock.MockBlockchainStore {
	mockStore := mock.NewMockBlockchainStore(ctrl)
	mockStore.EXPECT().ReadObject(gomock.Any(), gomock.Any()).DoAndReturn(
		func(contractName string, key []byte) ([]byte, error) {
			if contractName == errContract {
				return nil, fmt.Errorf("not find")
			}
			if bytes.Equal(key, []byte(native.BalanceKey(testAddr))) {
				return []byte(fmt.Sprintf("%d", testAddrBalance)), nil
			}
			if bytes.Equal(key, []byte(native.KeyMinSelfDelegation)) {
				return []byte("200000"), nil
			}
			if bytes.Equal(key, []byte(native.BalanceKey(native.StakeContractAddr()))) {
				return []byte("10000"), nil
			}
			if bytes.Equal(key, []byte(native.KeyCurrentEpoch)) {
				epoch := &syscontract.Epoch{
					EpochId:               8,
					NextEpochCreateHeight: 100,
					ProposerVector:        []string{"proposer1", "proposer2", "proposer3", "proposer4"},
				}
				bz, err := proto.Marshal(epoch)
				return bz, err
			}
			if bytes.Equal(key, []byte(native.KeyEpochBlockNumber)) {
				bz := make([]byte, 8)
				binary.BigEndian.PutUint64(bz, uint64(testEpochBlkNum))
				return bz, nil
			}
			if bytes.Equal(key, []byte(native.KeyEpochValidatorNumber)) {
				bz := make([]byte, 8)
				binary.BigEndian.PutUint64(bz, 4)
				return bz, nil
			}
			if contractName == syscontract.SystemContract_DPOS_STAKE.String() {
				switch string(key) {
				case string(native.ToNodeIDKey("proposer1")):
					return []byte("node_id1"), nil
				case string(native.ToNodeIDKey("proposer2")):
					return []byte("node_id2"), nil
				case string(native.ToNodeIDKey("proposer3")):
					return []byte("node_id3"), nil
				case string(native.ToNodeIDKey("proposer4")):
					return []byte("node_id4"), nil
				default:
					h := sha256.Sum256(key)
					return h[:], nil
				}
			}
			if contractName == syscontract.SystemContract_CHAIN_CONFIG.String() &&
				string(key) == syscontract.SystemContract_CHAIN_CONFIG.String() {
				cfg := &configPb.ChainConfig{
					ChainId: "test_chain",
					Consensus: &configPb.ConsensusConfig{
						Type: consensus.ConsensusType_DPOS,
						Nodes: []*configPb.OrgConfig{{
							OrgId:  dposOrgId,
							NodeId: []string{"node_id_1"},
						}},
					},
				}
				return proto.Marshal(cfg)
			}
			return nil, nil
		}).AnyTimes()
	return mockStore
}

//  mock function
func newMockWithIterStore(ctrl *gomock.Controller) *mock.MockBlockchainStore {
	cache := newCacheStore()
	mockBlockChainStore := baseMockStore(ctrl)

	mockBlockChainStore.EXPECT().PutBlock(gomock.Any(), gomock.Any()).DoAndReturn(
		func(block *common.Block, txRWSets []*common.TxRWSet) error {
			for _, rw := range txRWSets {
				for _, w := range rw.TxWrites {
					cache.putValue(w.Key, w.Value)
				}
			}
			return nil
		}).AnyTimes()
	mockBlockChainStore.EXPECT().SelectObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(contractName string, startKey []byte, limit []byte) (protocol.StateIterator, error) {
			return newCacheIter(startKey, limit, cache), nil
		}).AnyTimes()
	return mockBlockChainStore
}

type cacheStore struct {
	keyItems [][]byte
	values   map[string][]byte
}

//  mock function
func newCacheStore() *cacheStore {
	return &cacheStore{
		keyItems: make([][]byte, 0, 4),
		values:   make(map[string][]byte, 4),
	}
}

//  mock function
func (cache *cacheStore) putValue(key, val []byte) {
	cache.keyItems = append(cache.keyItems, key)
	sort.Slice(cache.keyItems, func(i, j int) bool {
		return bytes.Compare(cache.keyItems[i], cache.keyItems[j]) < 0
	})
	cache.values[string(key)] = val
}

// cacheIter cache iterator
type cacheIter struct {
	start  int
	end    int
	cursor int

	store *cacheStore
}

//  mock function
func newCacheIter(start []byte, end []byte, store *cacheStore) *cacheIter {
	iter := &cacheIter{store: store}
	s := sort.Search(len(iter.store.keyItems), func(i int) bool {
		return bytes.Compare(iter.store.keyItems[i], start) >= 0
	})
	e := sort.Search(len(iter.store.keyItems), func(i int) bool {
		return bytes.Compare(iter.store.keyItems[i], end) > 0
	})
	iter.start = s
	iter.end = e
	iter.cursor = s
	return iter
}

// Next returns true if there is the next value in the cache iterator, otherwise returns false
func (iter *cacheIter) Next() bool {
	if iter.start >= iter.end {
		return false
	}
	isValid := iter.start < iter.end
	iter.cursor = iter.start
	iter.start++
	return isValid
}

// Value returns the value in the cached iterator
func (iter *cacheIter) Value() (*store.KV, error) {
	if iter.cursor >= iter.end {
		return nil, fmt.Errorf("cursor:%d is out of limit:%d in the iter", iter.start, iter.end)
	}
	key := iter.store.keyItems[iter.cursor]
	val := iter.store.values[string(key)]
	return &store.KV{Key: key, Value: val}, nil
}

func (iter *cacheIter) Release() {
	//	no impl
}

//  mock function
func newMockChainConf(ctrl *gomock.Controller) protocol.ChainConf {
	mockConf := mock.NewMockChainConf(ctrl)
	mockConf.EXPECT().ChainConfig().Return(&configPb.ChainConfig{
		ChainId: "test_chain",
		Consensus: &configPb.ConsensusConfig{
			Type: consensus.ConsensusType_DPOS,
		},
	}).AnyTimes()
	return mockConf
}

//  mock function
func newMockLogger() protocol.Logger {
	return &test.GoLogger{}
}
