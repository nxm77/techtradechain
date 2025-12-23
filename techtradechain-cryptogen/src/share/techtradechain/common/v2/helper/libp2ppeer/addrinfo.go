package libp2ppeer

import (
	"fmt"

	ma "github.com/multiformats/go-multiaddr"
)

var ErrInvalidAddr = fmt.Errorf("invalid p2p multiaddr")

type AddrInfo struct {
	ID    ID
	Addrs []ma.Multiaddr
}

func (pi AddrInfo) String() string {
	return fmt.Sprintf("{%v: %v}", pi.ID, pi.Addrs)
}

// AddrInfoFromP2pAddr converts a Multiaddr to an AddrInfo.
func AddrInfoFromP2pAddr(m ma.Multiaddr) (*AddrInfo, error) {
	transport, id := SplitAddr(m)
	if id == "" {
		return nil, ErrInvalidAddr
	}
	info := &AddrInfo{ID: id}
	if transport != nil {
		info.Addrs = []ma.Multiaddr{transport}
	}
	return info, nil
}

// SplitAddr splits a p2p Multiaddr into a transport multiaddr and a peer ID.
//
// * Returns a nil transport if the address only contains a /p2p part.
// * Returns a empty peer ID if the address doesn't contain a /p2p part.
func SplitAddr(m ma.Multiaddr) (transport ma.Multiaddr, id ID) {
	if m == nil {
		return nil, ""
	}

	transport, p2ppart := ma.SplitLast(m)
	if p2ppart == nil || p2ppart.Protocol().Code != ma.P_P2P {
		return m, ""
	}
	id = ID(p2ppart.RawValue()) // already validated by the multiaddr library.
	return transport, id
}
