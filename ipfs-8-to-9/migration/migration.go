// package mg8 contains the code to perform 8-9 repository migration in
// go-ipfs. This performs a switch to raw multihashes for all keys in the
// go-ipfs datastore (https://github.com/ipfs/go-ipfs/issues/6815).
package mg8

import (
	"errors"
	"fmt"

	migrate "github.com/ipfs/fs-repo-migrations/go-migrate"
	lock "github.com/ipfs/fs-repo-migrations/ipfs-1-to-2/repolock"
	"github.com/ipfs/go-datastore/namespace"

	mfsr "github.com/ipfs/fs-repo-migrations/mfsr"
	log "github.com/ipfs/fs-repo-migrations/stump"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-ipfs/plugin/loader"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
)

// Migration implements the migration described above.
type Migration struct{}

// Versions returns the current version string for this migration.
func (m Migration) Versions() string {
	return "8-to-9"
}

// Reversible returns false. This migration cannot be reverted, as we do not
// know which raw hashes were actually CIDv1s. However, things should work all
// the same as they will be treated as CIDv0s in old versions anyways.
func (m Migration) Reversible() bool {
	return false
}

// Apply runs the migration.
func (m Migration) Apply(opts migrate.Options) error {
	log.Verbose = opts.Verbose
	log.Log("applying %s repo migration", m.Versions())

	log.VLog("locking repo at %q", opts.Path)
	lk, err := lock.Lock2(opts.Path)
	if err != nil {
		return err
	}
	defer lk.Close()

	repo := mfsr.RepoPath(opts.Path)

	log.VLog("  - verifying version is '8'")
	if err := repo.CheckVersion("8"); err != nil {
		return err
	}

	log.VLog("  - loading repo configurations")
	plugins, err := loader.NewPluginLoader(opts.Path)
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error injecting plugins: %s", err)
	}

	cfg, err := fsrepo.ConfigAt(opts.Path)
	if err != nil {
		return err
	}

	dsc, err := fsrepo.AnyDatastoreConfig(cfg.Datastore.Spec)
	if err != nil {
		return err
	}

	dstore, err := dsc.Create(opts.Path)
	if err != nil {
		return err
	}
	defer dstore.Close()

	// TODO: assuming the user has not modified this
	blocks := namespace.Wrap(dstore, ds.NewKey("/blocks"))

	log.VLog("  - starting CIDv1 to raw multihash block migration")
	cidSwapper := CidSwapper{blocks}
	total, err := cidSwapper.Run()
	if err != nil {
		log.Error(err)
		return err
	}

	log.Log("%d CIDv1 keys swapped to raw multihashes", total)
	if err := repo.WriteVersion("9"); err != nil {
		log.Error("failed to write version file")
		return err
	}
	log.Log("updated version file")

	return nil
}

// Revert attempts to undo the migration.
func (m Migration) Revert(opts migrate.Options) error {
	// TODO: Consider a no-op revert though
	return errors.New("This migration cannot be reverted")
}
