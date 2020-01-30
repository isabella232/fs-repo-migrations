package mg8

import (
	"errors"
	"sync"
	"sync/atomic"

	log "github.com/ipfs/fs-repo-migrations/stump"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/multiformats/go-multihash"
)

// SyncSize specifies how much we batch data before committing and syncing
// Increasing this number may result
var SyncSize uint64 = 10 * 1024 * 1024 // 1MiB

// NWorkers sets the number of batching threads to run
var NWorkers int = 4

// CidSwapper reads all the keys in a datastore and replaces
// them with their raw multihash.
type CidSwapper struct {
	Store ds.Batching // the datastore to migrate.
}

// Run lists all the keys in the datastore and triggers a swap operation for
// those corresponding to CIDv1s (replacing them by their raw multihash).
//
// Run returns the total number of keys swapped.
func (cswap *CidSwapper) Run() (uint64, error) {
	// Always perform a final sync
	defer cswap.Store.Sync(ds.NewKey("/"))
	// Query all keys. We will loop all keys
	// and swap those that can be parsed as CIDv1.
	queryAll := query.Query{
		KeysOnly: true,
	}

	results, err := cswap.Store.Query(queryAll)
	if err != nil {
		return 0, err
	}
	defer results.Close()
	resultsCh := results.Next()

	var total uint64
	var nErrors uint64
	var wg sync.WaitGroup
	wg.Add(NWorkers)
	for i := 0; i < NWorkers; i++ {
		go func() {
			defer wg.Done()
			n, e := cswap.swapWorker(resultsCh)
			atomic.AddUint64(&total, n)
			atomic.AddUint64(&nErrors, e)
		}()
	}
	wg.Wait()
	if nErrors > 0 {
		return total, errors.New("errors happened during the migration. Consider running it again")
	}

	return total, nil
}

// swapWorkers reads query results from a channel and renames CIDv1 keys to
// raw multihashes by reading the blocks and storing them with the new
// key. Returns the number of keys swapped and the number of errors.
func (cswap *CidSwapper) swapWorker(resultsCh <-chan query.Result) (uint64, uint64) {
	var swapped uint64
	var errored uint64
	var curSyncSize uint64

	// Process keys from the results channel
	for res := range resultsCh {
		if res.Error != nil {
			log.Error(res.Error)
			errored++
			continue
		}

		oldKey := ds.NewKey(res.Key)
		c, err := dsKeyToCid(oldKey)
		if err != nil {
			// complain if we find anything that is not a CID but
			// leave it as it is.
			log.Log("could not parse %s as a Cid", oldKey)
			continue
		}
		if c.Version() == 0 { // CidV0 are multihashes, leave them.
			continue
		}

		// Cid Version > 0
		newKey := multihashToDsKey(c.Hash())
		size, err := cswap.swap(oldKey, newKey)
		if err != nil {
			log.Error("swapping %s for %s: %s", oldKey, newKey, err)
			errored++
			continue
		}
		swapped++
		curSyncSize += size

		// Commit and Sync if we reached SyncSize
		if curSyncSize >= SyncSize {
			curSyncSize = 0
			err = cswap.Store.Sync(ds.NewKey("/"))
			if err != nil {
				log.Error(err)
				errored++
				continue
			}
		}
	}
	return swapped, errored
}

// swap swaps the old for the new key in a single transaction.
func (cswap *CidSwapper) swap(old, new ds.Key) (uint64, error) {
	// Unfortunately grouping multiple swaps in larger batches usually
	// results in "too many open files" errors in flatfs (many really
	// small files) . So we do small batches instead and Sync at larger
	// intervals.
	// Note flatfs will not clear up the batch after committing, so
	// the object cannot be-reused.
	batcher, err := cswap.Store.Batch()
	if err != nil {
		return 0, err
	}

	v, err := cswap.Store.Get(old)
	vLen := uint64(len(v))
	if err != nil {
		return vLen, err
	}
	if err := batcher.Put(new, v); err != nil {
		return vLen, err
	}
	if err := batcher.Delete(old); err != nil {
		return vLen, err
	}
	return vLen, batcher.Commit()
}

// Copied from go-ipfs-ds-help as that one is gone.
func dsKeyToCid(dsKey datastore.Key) (cid.Cid, error) {
	kb, err := dshelp.BinaryFromDsKey(dsKey)
	if err != nil {
		return cid.Cid{}, err
	}
	return cid.Cast(kb)
}

// multihashToDsKey creates a Key from the given Multihash.
// here to avoid dependency on newer dshelp function.
// TODO: can be removed if https://github.com/ipfs/go-ipfs-ds-help/pull/18
// is merged.
func multihashToDsKey(k multihash.Multihash) datastore.Key {
	return dshelp.NewKeyFromBinary(k)
}
