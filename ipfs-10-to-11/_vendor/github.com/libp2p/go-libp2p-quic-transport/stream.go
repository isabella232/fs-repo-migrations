package libp2pquic

import (
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/libp2p/go-libp2p-core/mux"

	quic "github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/lucas-clemente/quic-go"
)

const (
	reset quic.ErrorCode = 0
)

type stream struct {
	quic.Stream
}

func (s *stream) Read(b []byte) (n int, err error) {
	n, err = s.Stream.Read(b)
	if serr, ok := err.(quic.StreamError); ok && serr.Canceled() {
		err = mux.ErrReset
	}

	return n, err
}

func (s *stream) Write(b []byte) (n int, err error) {
	n, err = s.Stream.Write(b)
	if serr, ok := err.(quic.StreamError); ok && serr.Canceled() {
		err = mux.ErrReset
	}

	return n, err
}

func (s *stream) Reset() error {
	s.Stream.CancelRead(reset)
	s.Stream.CancelWrite(reset)
	return nil
}

func (s *stream) Close() error {
	s.Stream.CancelRead(reset)
	return s.Stream.Close()
}

func (s *stream) CloseRead() error {
	s.Stream.CancelRead(reset)
	return nil
}

func (s *stream) CloseWrite() error {
	return s.Stream.Close()
}

var _ mux.MuxedStream = &stream{}
