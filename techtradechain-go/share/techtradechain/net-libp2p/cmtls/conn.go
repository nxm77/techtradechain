/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package cmtls is about libp2p tls conn.
package cmtls

import (
	"net"

	"techtradechain.com/techtradechain/common/v2/crypto/tls"
	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/sec"
)

type conn struct {
	*tls.Conn

	localPeer peer.ID
	privKey   ci.PrivKey

	remotePeer   peer.ID
	remotePubKey ci.PubKey
}

var _ sec.SecureConn = &conn{}

func (c *conn) LocalPeer() peer.ID {
	return c.localPeer
}

func (c *conn) LocalPrivateKey() ci.PrivKey {
	return c.privKey
}

func (c *conn) RemotePeer() peer.ID {
	return c.remotePeer
}

func (c *conn) RemotePublicKey() ci.PubKey {
	return c.remotePubKey
}

type connTcp struct {
	net.Conn
	localPeer    peer.ID
	privKey      ci.PrivKey
	remotePeer   peer.ID
	remotePubKey ci.PubKey
}

var _ sec.SecureConn = (*connTcp)(nil)

func (c *connTcp) LocalPeer() peer.ID {
	return c.localPeer
}

func (c *connTcp) LocalPrivateKey() ci.PrivKey {
	return c.privKey
}

func (c *connTcp) RemotePeer() peer.ID {
	return c.remotePeer
}

func (c *connTcp) RemotePublicKey() ci.PubKey {
	return c.remotePubKey
}
