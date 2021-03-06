package peerresponsemanager

import (
	"context"

	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-graphsync/peermanager"
)

// PeerSenderFactory provides a function that will create a PeerResponseSender.
type PeerSenderFactory func(ctx context.Context, p peer.ID) PeerResponseSender

// PeerResponseManager manages message queues for peers
type PeerResponseManager struct {
	*peermanager.PeerManager
}

// New generates a new peer manager for sending responses
func New(ctx context.Context, createPeerSender PeerSenderFactory) *PeerResponseManager {
	return &PeerResponseManager{
		PeerManager: peermanager.New(ctx, func(ctx context.Context, p peer.ID) peermanager.PeerProcess {
			return createPeerSender(ctx, p)
		}),
	}
}

// SenderForPeer returns a response sender to use with the given peer
func (prm *PeerResponseManager) SenderForPeer(p peer.ID) PeerResponseSender {
	return prm.GetProcess(p).(PeerResponseSender)
}
