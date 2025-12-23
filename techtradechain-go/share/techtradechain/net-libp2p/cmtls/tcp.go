/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cmtls

import (
	"context"
	"errors"
	"net"

	"techtradechain.com/techtradechain/common/v2/crypto/tls"
	api "techtradechain.com/techtradechain/protocol/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/sec"
)

// TcpTransport TcpTransport .
//  @Description:
type TcpTransport struct {
	log                     api.Logger
	config                  *tls.Config
	localPeer               peer.ID
	privateKey              crypto.PrivKey
	LibP2pHost              func() host.Host
	httpTunnelTargetAddress map[string]string
}

var _ sec.SecureTransport = (*TcpTransport)(nil)

// NewTcpTransport .
//  @Description: return a function can create a new Transport instance.
//  @param tlsCfg
//  @param host
//  @param logger
//  @param
//  @return func(key crypto.PrivKey) (*TcpTransport, error)
func NewTcpTransport(tlsCfg *tls.Config, httpTunnelTargetAddress map[string]string,
	host func() host.Host, logger api.Logger,
) func(key crypto.PrivKey) (*TcpTransport, error) {
	return func(key crypto.PrivKey) (*TcpTransport, error) {
		id, err := peer.IDFromPrivateKey(key)
		if err != nil {
			return nil, err
		}
		return &TcpTransport{
			config:                  tlsCfg,
			privateKey:              key,
			localPeer:               id,
			LibP2pHost:              host,
			httpTunnelTargetAddress: httpTunnelTargetAddress,
			log:                     logger,
		}, nil
	}
}

// SecureInbound SecureInbound .
//  @Description:
//  @receiver t
//  @param ctx
//  @param insecure
//  @return sec.SecureConn
//  @return error
func (t *TcpTransport) SecureInbound(ctx context.Context, insecure net.Conn) (sec.SecureConn, error) {
	if insecure == nil {
		return nil, errors.New("insecure == nil")
	}
	if len(t.config.Certificates) == 0 || len(t.config.Certificates[0].Certificate) == 0 {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, errors.New("certificates == 0")
	}
	t.log.Info("tcp serverHandshake:", insecure.RemoteAddr().String())
	handShakeResult, err := serverHandshake(insecure, t.config.Clone(), t.log)
	if err != nil {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	remotePeerID, err := peer.IDFromPublicKey(handShakeResult.pk)
	if err != nil {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	return &connTcp{
		Conn:         insecure,
		localPeer:    t.localPeer,
		privKey:      t.privateKey,
		remotePeer:   remotePeerID,
		remotePubKey: handShakeResult.pk,
	}, nil

}

//  SecureOutbound .
//  @Description:
//  @receiver t
//  @param ctx
//  @param insecure
//  @param p
//  @return sec.SecureConn
//  @return error
func (t *TcpTransport) SecureOutbound(ctx context.Context, insecure net.Conn, p peer.ID) (sec.SecureConn, error) {
	if insecure == nil {
		return nil, errors.New("insecure == nil")
	}
	if len(t.config.Certificates) == 0 || len(t.config.Certificates[0].Certificate) == 0 {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, errors.New("certificates == 0")
	}
	//
	targetAddress, ok := t.httpTunnelTargetAddress[p.Pretty()]
	if ok {
		t.log.Infof("send httpTunnel proxy(%v)target(%v)", insecure.RemoteAddr().String(), targetAddress)
		err := sendHttpTunnelReq(insecure, targetAddress)
		if err != nil {
			errClose := insecure.Close()
			if errClose != nil {
				t.log.Warn("conn Close err ", errClose)
			}
			return nil, err
		}
	}
	//
	t.log.Info("tcp clientHandshake:", insecure.RemoteAddr().String())
	handShakeResult, err := clientHandshake(insecure, t.config.Clone(), t.log)
	if err != nil {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	remotePeerID, err := peer.IDFromPublicKey(handShakeResult.pk)
	if err != nil {
		errClose := insecure.Close()
		if errClose != nil {
			t.log.Warn("conn Close err ", errClose)
		}
		return nil, err
	}
	return &connTcp{
		Conn:         insecure,
		localPeer:    t.localPeer,
		privKey:      t.privateKey,
		remotePeer:   remotePeerID,
		remotePubKey: handShakeResult.pk,
	}, nil
}
