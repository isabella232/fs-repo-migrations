package mg10

import (
	"context"
	"fmt"
	"path"

	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-blockservice"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-datastore"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-filestore"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-ipfs-exchange-offline"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-ipfs-pinner/pinconv"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-ipfs/plugin/loader"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-ipfs/repo/fsrepo"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-ipld-format"
	"github.com/ipfs/fs-repo-migrations/ipfs-10-to-11/_vendor/github.com/ipfs/go-merkledag"

	migrate "github.com/ipfs/fs-repo-migrations/go-migrate"
	lock "github.com/ipfs/fs-repo-migrations/ipfs-1-to-2/repolock"
	mfsr "github.com/ipfs/fs-repo-migrations/mfsr"
	log "github.com/ipfs/fs-repo-migrations/stump"
)

type Migration struct{}

func (m Migration) Versions() string {
	return "10-to-11"
}

func (m Migration) Reversible() bool {
	return true
}

func (m Migration) Apply(opts migrate.Options) error {
	log.Verbose = opts.Verbose
	log.Log("applying %s repo migration", m.Versions())

	log.VLog("locking repo at %q", opts.Path)
	lk, err := lock.Lock2(opts.Path)
	if err != nil {
		return err
	}
	defer lk.Close()

	if err = setupPlugins(opts.Path); err != nil {
		log.Error("failed to setup plugins", err.Error())
		return err
	}

	// Set to previous version to avoid "needs migration" error.  This is safe
	// for this migration since repo has not changed.
	fsrepo.RepoVersion = 10

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err = transferPins(ctx, opts.Path); err != nil {
		log.Error("failed to transfer pins:", err.Error())
		return err
	}

	err = mfsr.RepoPath(opts.Path).WriteVersion("11")
	if err != nil {
		log.Error("failed to update version file to 11")
		return err
	}

	log.Log("updated version file")
	return nil
}

func (m Migration) Revert(opts migrate.Options) error {
	log.Verbose = opts.Verbose
	log.Log("reverting migration")

	lk, err := lock.Lock2(opts.Path)
	if err != nil {
		return err
	}
	defer lk.Close()

	if err = setupPlugins(opts.Path); err != nil {
		log.Error("failed to setup plugins", err.Error())
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err = revertPins(ctx, opts.Path); err != nil {
		return err
	}

	err = mfsr.RepoPath(opts.Path).WriteVersion("10")
	if err != nil {
		log.Error("failed to update version file to 10")
		return err
	}

	log.Log("updated version file")
	return nil
}

type syncDagService struct {
	format.DAGService
	syncFn func() error
}

func (s *syncDagService) Sync() error {
	return s.syncFn()
}

type batchWrap struct {
	datastore.Datastore
}

func (d *batchWrap) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(d), nil
}

func setupPlugins(externalPluginsPath string) error {
	// Load any external plugins if available on externalPluginsPath
	plugins, err := loader.NewPluginLoader(path.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	// Load preloaded and external plugins
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

func makeStore(r repo.Repo) (datastore.Datastore, format.DAGService, format.DAGService, error) {
	dstr := r.Datastore()
	dstore := &batchWrap{dstr}

	bstore := blockstore.NewBlockstore(dstr)
	bserv := blockservice.New(bstore, offline.Exchange(bstore))
	dserv := merkledag.NewDAGService(bserv)
	internalDag := merkledag.NewDAGService(bserv)

	syncFn := func() error {
		err := dstore.Sync(blockstore.BlockPrefix)
		if err != nil {
			return fmt.Errorf("cannot sync blockstore: %v", err)
		}
		err = dstore.Sync(filestore.FilestorePrefix)
		if err != nil {
			return fmt.Errorf("cannot sync filestore: %v", err)
		}
		return nil
	}
	syncDs := &syncDagService{dserv, syncFn}
	syncInternalDag := &syncDagService{internalDag, syncFn}

	return dstore, syncDs, syncInternalDag, nil
}

func transferPins(ctx context.Context, repopath string) error {
	log.Log("> Upgrading pinning to use datastore")

	if !fsrepo.IsInitialized(repopath) {
		return fmt.Errorf("ipfs repo %q not initialized", repopath)
	}

	log.VLog("  - opening datastore at %q", repopath)
	r, err := fsrepo.Open(repopath)
	if err != nil {
		return fmt.Errorf("cannot open datastore: %v", err)
	}
	defer r.Close()

	dstore, dserv, internalDag, err := makeStore(r)
	if err != nil {
		return err
	}

	log.Log("  - importing from ipld pinner")

	_, toDSCount, err := pinconv.ConvertPinsFromIPLDToDS(ctx, dstore, dserv, internalDag)
	if err != nil {
		log.Error("failed to convert ipld pin data into datastore")
		return err
	}
	log.Log("  - converted %d pins from ipld storage into datastore", toDSCount)
	return nil
}

func revertPins(ctx context.Context, repopath string) error {
	log.Log("> Reverting pinning to use ipld storage")

	log.VLog("  - opening datastore at %q", repopath)
	r, err := fsrepo.Open(repopath)
	if err != nil {
		return fmt.Errorf("cannot open datastore: %v", err)
	}
	defer r.Close()

	dstore, dserv, internalDag, err := makeStore(r)
	if err != nil {
		return err
	}

	_, toIPLDCount, err := pinconv.ConvertPinsFromDSToIPLD(ctx, dstore, dserv, internalDag)
	if err != nil {
		log.Error("failed to conver pin data from datastore to ipld pinner")
		return err
	}
	log.Log("converted %d pins from datastore to ipld storage", toIPLDCount)
	return nil
}
