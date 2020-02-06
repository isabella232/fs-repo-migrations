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
)

// SyncSize specifies how much we batch data before committing and syncing.
var SyncSize uint64 = 10 * 1024 * 1024 // 10MiB

// NWorkers sets the number of swapping threads to run.
var NWorkers int = 4

// Swap holds the datastore keys for the original CID and for the
// destination Multihash.
type Swap struct {
	Old ds.Key
	New ds.Key
}

// CidSwapper reads all the keys in a datastore and replaces
// them with their raw multihash.
type CidSwapper struct {
	Store  ds.Batching // the datastore to migrate.
	SwapCh chan Swap   // a channel that gets notified for every swap
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

// Revert allows to undo any operations made by Run(). The given channel should
// receive Swap objects as they were sent by Run. It returns the number of
// swap operations performed.
func (cswap *CidSwapper) Revert(unswapCh <-chan Swap) (uint64, error) {
	var total uint64
	var nErrors uint64
	var wg sync.WaitGroup
	wg.Add(NWorkers)
	for i := 0; i < NWorkers; i++ {
		go func() {
			defer wg.Done()
			n, e := cswap.unswapWorker(unswapCh)
			atomic.AddUint64(&total, n)
			atomic.AddUint64(&nErrors, e)
		}()
	}
	wg.Wait()
	if nErrors > 0 {
		return total, errors.New("errors happened during the revert migration. Consider running it again")
	}

	return total, nil
}

// swapWorkers reads query results from a channel and renames CIDv1 keys to
// raw multihashes by reading the blocks and storing them with the new
// key. Returns the number of keys swapped and the number of errors.
func (cswap *CidSwapper) swapWorker(resultsCh <-chan query.Result) (uint64, uint64) {
	var errored uint64

	sw := &swapWorker{
		store:  cswap.Store,
		swapCh: cswap.SwapCh,
	}

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
		mh := c.Hash()
		newKey := dshelp.MultihashToDsKey(mh)
		err = sw.swap(oldKey, newKey)
		if err != nil {
			log.Error("swapping %s for %s: %s", oldKey, newKey, err)
			errored++
			continue
		}
	}

	// final sync
	err := sw.sync()
	if err != nil {
		log.Error("error performing last sync: %s", err)
		errored++
	}

	return sw.swapped, errored
}

// unswap worker takes notifications from unswapCh (as they would be sent by
// the swapWorker) and undoes them. It ignores NotFound errors so that reverts
// can succeed even if they failed half-way.
func (cswap *CidSwapper) unswapWorker(unswapCh <-chan Swap) (uint64, uint64) {
	var errored uint64

	swker := &swapWorker{
		store:  cswap.Store,
		swapCh: cswap.SwapCh,
	}

	// Process keys from the results channel
	for sw := range unswapCh {
		err := swker.swap(sw.New, sw.Old)
		if err == ds.ErrNotFound {
			log.Log("could not revert %s->%s. Was it already reverted? Ignoring...", sw.Old, sw.New)
			continue
		}
		if err != nil {
			log.Error("swapping %s for %s: %s", sw.New, sw.Old, err)
			errored++
			continue
		}
	}

	// final sync
	err := swker.sync()
	if err != nil {
		log.Error("error performing last sync: %s", err)
		errored++
	}

	return swker.swapped, errored
}

// swapWorker swaps old keys for new keys, syncing to disk regularly
// and notifying swapCh of the changes.
type swapWorker struct {
	swapped     uint64
	curSyncSize uint64

	swapCh chan Swap
	store  ds.Batching

	toDelete []ds.Key
}

// swap replaces old keys with new ones. It Syncs() when the
// number of items written reaches SyncSize. Upon that it proceeds
// to delete the old items.
func (sw *swapWorker) swap(old, new ds.Key) error {
	v, err := sw.store.Get(old)
	vLen := uint64(len(v))
	if err != nil {
		return err
	}
	if err := sw.store.Put(new, v); err != nil {
		return err
	}
	sw.toDelete = append(sw.toDelete, old)

	sw.swapped++
	sw.curSyncSize += vLen

	if sw.swapCh != nil {
		sw.swapCh <- Swap{Old: old, New: new}
	}

	// We have copied about 10MB
	if sw.curSyncSize >= SyncSize {
		sw.curSyncSize = 0
		err = sw.sync()
		if err != nil {
			return err
		}
	}
	return nil
}

func (sw *swapWorker) sync() error {
	// Sync all the new keys to disk
	err := sw.store.Sync(ds.NewKey("/"))
	if err != nil {
		log.Error(err)
		return err
	}

	// Delete all the old keys
	for _, o := range sw.toDelete {
		if err := sw.store.Delete(o); err != nil {
			return err
		}
	}
	sw.toDelete = nil

	// Sync again.
	err = sw.store.Sync(ds.NewKey("/"))
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// Copied from go-ipfs-ds-help as that one is gone.
func dsKeyToCid(dsKey datastore.Key) (cid.Cid, error) {
	kb, err := dshelp.BinaryFromDsKey(dsKey)
	if err != nil {
		return cid.Cid{}, err
	}
	return cid.Cast(kb)
}
