package badger

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/dgraph-io/badger"
)

type Batch struct {
	store   *Store
	merge   *store.EmulatedMerge
	entries []*badger.Entry
}

func (b *Batch) Set(key, val []byte) {
	keyc := make([]byte, len(key))
	copy(keyc, key)

	valc := make([]byte, len(val))
	copy(valc, val)

	b.entries = badger.EntriesSet(b.entries, keyc, valc)
}

func (b *Batch) Delete(key []byte) {
	keyc := make([]byte, len(key))
	copy(keyc, key)

	b.entries = badger.EntriesDelete(b.entries, keyc)
}

func (b *Batch) Merge(key, val []byte) {
	b.merge.Merge(key, val)
}

func (b *Batch) Reset() {
	b.entries = nil
	b.merge = store.NewEmulatedMerge(b.store.mo)
}

func (b *Batch) Close() error {
	b.entries = nil
	b.merge = nil
	return nil
}
