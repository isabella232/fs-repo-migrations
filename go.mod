module github.com/ipfs/fs-repo-migrations

go 1.13

replace github.com/ipfs/fs-repo-migrations/ipfs-8-to-9/migration => ./ipfs-8-to-9/migration

require (
	github.com/ipfs/fs-repo-migrations/ipfs-8-to-9/migration v0.0.0-00010101000000-000000000000
	github.com/mitchellh/go-homedir v1.1.0
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
)
