module github.com/ipfs/fs-repo-migrations/ipfs-10-to-11

go 1.14

replace github.com/ipfs/fs-repo-migrations/tools => ../tools

require (
	github.com/ipfs/fs-repo-migrations/tools v0.0.1
	github.com/ipfs/go-blockservice v0.1.4
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-filestore v0.0.3
	github.com/ipfs/go-ipfs v0.7.1-0.20201204203331-9bc8f6d5b514
	github.com/ipfs/go-ipfs-blockstore v0.1.4
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipfs-pinner v0.1.0
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-merkledag v0.3.2
)
