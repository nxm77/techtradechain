package techtradechain_sdk_go

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
)

type proxyDialer interface {
	dial(ctx context.Context, addr string) (net.Conn, error)
}

type grpcProxyDialer struct {
	proxyAddress string
}

func newGRPCProxyDialer(proxyURL *url.URL) proxyDialer {
	return &grpcProxyDialer{
		proxyAddress: proxyURL.Host,
	}
}

func (g *grpcProxyDialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", g.proxyAddress)
	if err != nil {
		return nil, err
	}

	err = g.proxyConnect(ctx, conn, addr)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (g *grpcProxyDialer) proxyConnect(ctx context.Context, conn net.Conn, targetAddr string) error {
	req := g.createConnectRequest(ctx, targetAddr)
	if err := req.Write(conn); err != nil {
		return err
	}

	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to connect proxy, status : %s", resp.Status)
	}
	return nil
}

func (g *grpcProxyDialer) createConnectRequest(ctx context.Context, targetAddress string) *http.Request {
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: targetAddress},
	}

	return req.WithContext(ctx)
}
