package util

import "github.com/ipfs/fs-repo-migrations/ipfs-3-to-4/Godeps/_workspace/src/golang.org/x/net/context"

func ContextDo(ctx context.Context, f func() error) error {

	ch := make(chan error)

	go func() {
		select {
		case <-ctx.Done():
		case ch <- f():
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case val := <-ch:
		return val
	}
	return nil
}
