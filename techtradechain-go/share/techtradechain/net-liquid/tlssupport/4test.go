/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tlssupport

import (
	"context"
	"strconv"

	"techtradechain.com/techtradechain/net-liquid/relay"

	"techtradechain.com/techtradechain/common/v2/crypto"
	"techtradechain.com/techtradechain/common/v2/crypto/asym"
	"techtradechain.com/techtradechain/net-liquid/core/host"
	"techtradechain.com/techtradechain/net-liquid/core/peer"
	host2 "techtradechain.com/techtradechain/net-liquid/host"
	api "techtradechain.com/techtradechain/protocol/v2"
	ma "github.com/multiformats/go-multiaddr"
)

// CreateHostRandom create a host instance with random crypto.
// This method only for testing.
func CreateHostRandom(
	hostSeq int,
	ip string,
	seeds map[peer.ID]ma.Multiaddr,
	logger api.Logger) (host.Host, peer.ID, ma.Multiaddr, error) {
	privateKey, err := asym.GenerateKeyPair(crypto.RSA2048)
	if err != nil {
		return nil, "", nil, err
	}
	return CreateHostWithCrypto(hostSeq, ip, privateKey, seeds, logger)
}

// CreateHostWithCrypto create a host instance with quic network type and simple config.
// This method only for testing.
func CreateHostWithCrypto(
	hostSeq int,
	ip string,
	privateKey crypto.PrivateKey,
	seeds map[peer.ID]ma.Multiaddr,
	logger api.Logger) (host.Host, peer.ID, ma.Multiaddr, error) {
	tlsCfg, loadPidFunc, err := MakeTlsConfigAndLoadPeerIdFuncWithPrivateKey(privateKey)
	if err != nil {
		return nil, "", nil, err
	}
	//r, _ := rand.Int(rand.Reader, big.NewInt(int64(20)))
	//random := int(r.Int64())
	addrs := []ma.Multiaddr{ma.StringCast("/ip4/" + ip + "/tcp/" + strconv.Itoa(9000+hostSeq))}
	hostCfg := &host2.HostConfig{
		TlsCfg:                    tlsCfg,
		LoadPidFunc:               loadPidFunc,
		SendStreamPoolInitSize:    10,
		SendStreamPoolCap:         100,
		PeerReceiveStreamMaxCount: 100,
		ListenAddresses:           addrs,
		DirectPeers:               seeds,
		MsgCompress:               false,
		PrivateKey:                privateKey,
	}
	h, err := hostCfg.NewHost(context.Background(), host2.TcpNetwork, logger, relay.OptHop)
	if err != nil {
		return nil, "", nil, err
	}
	return h, h.ID(), addrs[0], nil
}

// CreateHostWithCrypto2 create a host instance with quic network type and simple config.
// This method only for testing.
func CreateHostWithCrypto2(
	hostSeq int,
	ip string,
	privateKey crypto.PrivateKey,
	seeds map[peer.ID]ma.Multiaddr,
	logger api.Logger) (host.Host, peer.ID, ma.Multiaddr, error) {
	tlsCfg, loadPidFunc, err := MakeTlsConfigAndLoadPeerIdFuncWithPrivateKey(privateKey)
	if err != nil {
		return nil, "", nil, err
	}
	//r, _ := rand.Int(rand.Reader, big.NewInt(int64(20)))
	//random := int(r.Int64())
	addrs := []ma.Multiaddr{ma.StringCast("/ip4/" + ip + "/tcp/" + strconv.Itoa(9111+hostSeq))}
	hostCfg := &host2.HostConfig{
		TlsCfg:                    tlsCfg,
		LoadPidFunc:               loadPidFunc,
		SendStreamPoolInitSize:    10,
		SendStreamPoolCap:         100,
		PeerReceiveStreamMaxCount: 100,
		ListenAddresses:           addrs,
		DirectPeers:               seeds,
		MsgCompress:               false,
		PrivateKey:                privateKey,
	}
	h, err := hostCfg.NewHost(context.Background(), host2.TcpNetwork, logger, relay.OptHop)
	if err != nil {
		return nil, "", nil, err
	}
	return h, h.ID(), addrs[0], nil
}
