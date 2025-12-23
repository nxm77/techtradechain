package csms

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/sec"
	mss "github.com/multiformats/go-multistream"
)

// SSMuxer is a multistream stream security transport multiplexer.
//
// SSMuxer is safe to use without initialization. However, it's not safe to move
// after use.
type SSMuxer struct {
	mux             mss.MultistreamMuxer
	tpts            map[string]sec.SecureTransport
	tptSlice        []*secureTransport
	OrderPreference []string
}

var _ sec.SecureTransport = (*SSMuxer)(nil)

type secureTransport struct {
	tag    string
	handle sec.SecureTransport
}

const maxTransport = 5
const envNegotiateName = "LIBP2P_DISABLE_SECURE_NEGOTIATE"

// AddTransport adds a stream security transport to this multistream muxer.
//
// This method is *not* thread-safe. It should be called only when initializing
// the SSMuxer.
func (sm *SSMuxer) AddTransport(path string, transport sec.SecureTransport) {
	if sm.tpts == nil {
		sm.tpts = make(map[string]sec.SecureTransport, 1)
	}
	if sm.tptSlice == nil {
		sm.tptSlice = make([]*secureTransport, 1)
	}
	if len(sm.tpts) < maxTransport {
		sm.tptSlice = append(sm.tptSlice, &secureTransport{path, transport})
	}
	sm.mux.AddHandler(path, nil)
	sm.tpts[path] = transport
	sm.OrderPreference = append(sm.OrderPreference, path)
}

// SecureInbound secures an inbound connection using this multistream
// multiplexed stream security transport.
func (sm *SSMuxer) SecureInbound(ctx context.Context, insecure net.Conn) (sec.SecureConn, error) {
	disableNegotiate := os.Getenv(envNegotiateName)
	if disableNegotiate == "1" {
		return sm.secureInboundWithoutNegotiate(ctx, insecure)
	}
	return sm.secureInbound(ctx, insecure)
}

// SecureOutbound secures an outbound connection using this multistream
// multiplexed stream security transport.
func (sm *SSMuxer) SecureOutbound(ctx context.Context, insecure net.Conn, p peer.ID) (sec.SecureConn, error) {
	if targetAddress := getSendHttpTunnelTargetAddress(p.Pretty()); len(targetAddress) > 0 {
		err := sendHttpTunnelReq(insecure, targetAddress)
		if err != nil {
			fmt.Println("sendHttpTunnelReq err:", err)
			return nil, err
		}
	}
	disableNegotiate := os.Getenv(envNegotiateName)
	if disableNegotiate == "1" {
		return sm.secureOutboundWithoutNegotiate(ctx, insecure, p)
	}
	return sm.secureOutbound(ctx, insecure, p)
}

// SecureInbound secures an inbound connection using this multistream
// multiplexed stream security transport.
func (sm *SSMuxer) secureInbound(ctx context.Context, insecure net.Conn) (sec.SecureConn, error) {
	tpt, err := sm.selectProto(ctx, insecure, true)
	if err != nil {
		return nil, err
	}
	return tpt.SecureInbound(ctx, insecure)
}

// SecureOutbound secures an outbound connection using this multistream
// multiplexed stream security transport.
func (sm *SSMuxer) secureOutbound(ctx context.Context, insecure net.Conn, p peer.ID) (sec.SecureConn, error) {
	tpt, err := sm.selectProto(ctx, insecure, false)
	if err != nil {
		return nil, err
	}
	return tpt.SecureOutbound(ctx, insecure, p)
}

func (sm *SSMuxer) selectProto(ctx context.Context, insecure net.Conn, server bool) (sec.SecureTransport, error) {
	var proto string
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		if server {
			proto, _, err = sm.mux.Negotiate(insecure)
		} else {
			proto, err = mss.SelectOneOf(sm.OrderPreference, insecure)
		}
	}()

	select {
	case <-done:
		if err != nil {
			return nil, err
		}
		if tpt, ok := sm.tpts[proto]; ok {
			return tpt, nil
		}
		return nil, fmt.Errorf("selected unknown security transport")
	case <-ctx.Done():
		// We *must* do this. We have outstanding work on the connection
		// and it's no longer safe to use.
		insecure.Close()
		<-done // wait to stop using the connection.
		return nil, ctx.Err()
	}
}

func (sm *SSMuxer) secureInboundWithoutNegotiate(ctx context.Context, insecure net.Conn) (sec.SecureConn, error) {
	var s sec.SecureConn
	var err error
	for _, tpt := range sm.tptSlice {
		if tpt == nil {
			continue
		}
		s, err = tpt.handle.SecureInbound(ctx, insecure)
		if err == nil {
			break
		}
	}
	return s, err
}

func (sm *SSMuxer) secureOutboundWithoutNegotiate(ctx context.Context, insecure net.Conn, p peer.ID) (sec.SecureConn, error) {
	var s sec.SecureConn
	var err error
	for _, tpt := range sm.tptSlice {
		if tpt == nil {
			continue
		}
		s, err = tpt.handle.SecureOutbound(ctx, insecure, p)
		if err == nil {
			return s, err
		}
	}
	return s, err
}
