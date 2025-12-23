/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"context"
	"errors"
	"fmt"
	"net"

	"techtradechain.com/techtradechain/common/v2/crypto/tls"
	"techtradechain.com/techtradechain/net-libp2p/utils"
	api "techtradechain.com/techtradechain/protocol/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/sec"
	ma "github.com/multiformats/go-multiaddr"
)

// ID is the protocol ID (used when negotiating with multistream)
const ID = "/cmtls/1.0.0"

// Transport constructs secure communication sessions for a peer.
type Transport struct {
	config                  *tls.Config
	privKey                 crypto.PrivKey
	localPeer               peer.ID
	LibP2pHost              func() host.Host
	log                     api.Logger
	httpTunnelTargetAddress map[string]string
}

var _ sec.SecureTransport = &Transport{}

// New return a function can create a new Transport instance.
func New(
	tlsCfg *tls.Config,
	httpTunnelTargetAddress map[string]string,
	host func() host.Host,
	logger api.Logger,
) func(key crypto.PrivKey) (*Transport, error) {
	return func(key crypto.PrivKey) (*Transport, error) {
		id, err := peer.IDFromPrivateKey(key)
		if err != nil {
			return nil, err
		}
		return &Transport{
			config:                  tlsCfg,
			privKey:                 key,
			localPeer:               id,
			LibP2pHost:              host,
			httpTunnelTargetAddress: httpTunnelTargetAddress,
			log:                     logger,
		}, nil
	}
}

// SecureInbound runs the TLS handshake as a server.
func (t *Transport) SecureInbound(ctx context.Context, insecure net.Conn) (sec.SecureConn, error) {
	if insecure == nil {
		return nil, errors.New("insecure == nil")
	}
	c := tls.Server(insecure, t.config.Clone())
	if err := c.Handshake(); err != nil {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	// adapter handshake version
	bufConn := newMsgBufferConn(c)
	handshakeVer, err := checkHandshakeVersion(bufConn)
	if err != nil {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	// 234 handshake
	if handshakeVer == versionLEv234 {
		t.log.Info("234 tls serverHandshake:", insecure.RemoteAddr().String())
		var remotePubKey crypto.PubKey
		remotePubKey, err = t.getPeerPubKey(c)
		if err != nil {
			errClose := insecure.Close()
			if errClose != nil {
				t.log.Warn("conn Close err ", errClose)
			}
			return nil, err
		}
		return t.setupBufConn(bufConn, remotePubKey)
	}
	// 235 handshake
	t.log.Info("235 tls serverHandshake:", insecure.RemoteAddr().String())
	handShakeResult, err := serverHandshake(bufConn, t.config.Clone(), t.log)
	if err != nil {
		t.log.Errorf("235 tls serverHandshake:%v,err:%v", insecure.RemoteAddr().String(), err)
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	remotePubKey := handShakeResult.pk
	return t.setupConn(c, remotePubKey)
}

// SecureOutbound runs the TLS handshake as a client.
func (t *Transport) SecureOutbound(ctx context.Context, insecure net.Conn, p peer.ID) (sec.SecureConn, error) {
	if insecure == nil {
		return nil, errors.New("insecure == nil")
	}
	conf := t.config.Clone()

	host := t.LibP2pHost()
	if host != nil && host.Peerstore() != nil {
		peerInfo := host.Peerstore().PeerInfo(p)
		t.log.Info("SecureOutbound peerInfo:", p.String(), ",", peerInfo)
		for _, addr := range peerInfo.Addrs {
			if addr == nil {
				continue
			}
			t.log.Debug("SecureOutbound addr:", addr.String())
			if !haveDns(addr) {
				continue
			}
			t.log.Debug("SecureOutbound addr have dns:", addr.String())
			dnsDomain, _ := ma.SplitFirst(addr)
			if dnsDomain == nil {
				continue
			}
			conf.ServerName, _ = dnsDomain.ValueForProtocol(dnsDomain.Protocol().Code)
			t.log.Info("SecureOutbound ServerName:", conf.ServerName)
			break
		}
	}
	c := tls.Client(insecure, conf)
	if err := c.Handshake(); err != nil {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	//
	targetAddress, ok := t.httpTunnelTargetAddress[p.Pretty()]
	if ok {
		t.log.Infof("send httpTunnel proxy(%v)target(%v)", insecure.RemoteAddr().String(), targetAddress)
		err := sendHttpTunnelReq(c, targetAddress)
		if err != nil {
			errClose := insecure.Close()
			if errClose != nil {
				t.log.Warn("conn Close err ", errClose)
			}
			return nil, err
		}
	}
	//handshake
	t.log.Info("tls clientHandshake:", insecure.RemoteAddr().String())
	handShakeResult, err := clientHandshake(c, t.config.Clone(), t.log)
	if err != nil {
		t.log.Warnf("clientHandshake(%v) err:%v", c.RemoteAddr().String(), err)
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	remotePubKey := handShakeResult.pk
	return t.setupConn(c, remotePubKey)
}

//  haveDns .
//  @Description:
//  @param addr
//  @return bool
func haveDns(addr ma.Multiaddr) bool {
	protocols := addr.Protocols()
	for _, p := range protocols {
		switch p.Code {
		case ma.P_DNS, ma.P_DNS4, ma.P_DNS6:
			return true
		}
	}
	return false
}

func (t *Transport) getPeerPubKey(conn *tls.Conn) (crypto.PubKey, error) {
	state := conn.ConnectionState()
	if len(state.PeerCertificates) <= 0 {
		return nil, errors.New("expected one certificates in the chain")
	}
	pubKey, err := utils.ParsePublicKeyToPubKey(state.PeerCertificates[0].PublicKey.ToStandardKey())
	if err != nil {
		return nil, fmt.Errorf("unmarshalling public key failed: %s", err)
	}
	return pubKey, err
}

func (t *Transport) setupConn(tlsConn *tls.Conn, remotePubKey crypto.PubKey) (sec.SecureConn, error) {
	remotePeerID, err := peer.IDFromPublicKey(remotePubKey)
	if err != nil {
		errClose := tlsConn.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}

	return &conn{
		Conn:         tlsConn,
		localPeer:    t.localPeer,
		privKey:      t.privKey,
		remotePeer:   remotePeerID,
		remotePubKey: remotePubKey,
	}, nil
}

//  setupBufConn .
//  @Description:
//  @receiver t
//  @param conn
//  @param remotePubKey
//  @return sec.SecureConn
//  @return error
func (t *Transport) setupBufConn(conn *msgBufferConn, remotePubKey crypto.PubKey) (sec.SecureConn, error) {
	remotePeerID, err := peer.IDFromPublicKey(remotePubKey)
	if err != nil {
		errClose := conn.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	return &connTcp{
		Conn:         conn,
		localPeer:    t.localPeer,
		privKey:      t.privKey,
		remotePeer:   remotePeerID,
		remotePubKey: remotePubKey,
	}, nil
}
