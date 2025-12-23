/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package types

import (
	cmx509 "techtradechain.com/techtradechain/common/v2/crypto/x509"
	"techtradechain.com/techtradechain/net-liquid/core/peer"
)

// LoadPeerIdFromCMTlsCertFunc is a function can load the peer.ID
// from []*cmx509.Certificate exchanged during tls handshaking.
type LoadPeerIdFromCMTlsCertFunc func([]*cmx509.Certificate) (peer.ID, error)
