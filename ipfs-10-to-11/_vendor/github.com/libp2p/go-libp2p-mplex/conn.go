package peerstream_multiplex

import (
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/libp2p/go-libp2p-core/mux"
	mp "github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/libp2p/go-mplex"
)

type conn mp.Multiplex

func (c *conn) Close() error {
	return c.mplex().Close()
}

func (c *conn) IsClosed() bool {
	return c.mplex().IsClosed()
}

// OpenStream creates a new stream.
func (c *conn) OpenStream() (mux.MuxedStream, error) {
	s, err := c.mplex().NewStream()
	if err != nil {
		return nil, err
	}
	return (*stream)(s), nil
}

// AcceptStream accepts a stream opened by the other side.
func (c *conn) AcceptStream() (mux.MuxedStream, error) {
	s, err := c.mplex().Accept()
	if err != nil {
		return nil, err
	}
	return (*stream)(s), nil
}

func (c *conn) mplex() *mp.Multiplex {
	return (*mp.Multiplex)(c)
}

var _ mux.MuxedConn = &conn{}
