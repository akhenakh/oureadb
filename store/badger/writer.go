package badger

import (
	"fmt"

	"github.com/akhenakh/oureadb/store"
	"github.com/dgraph-io/badger"
)

type Writer struct {
	s *Store
}

func (w *Writer) NewBatch() store.KVBatch {
	return &Batch{
		store: w.s,
		merge: store.NewEmulatedMerge(w.s.mo),
	}
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(b store.KVBatch) error {
	batch, ok := b.(*Batch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	// first process merges
	for k, mergeOps := range batch.merge.Merges {
		kb := []byte(k)
		item := &badger.KVItem{}
		err := w.s.kv.Get(kb, item)
		if err != nil {
			return err
		}
		mergedVal, fullMergeOk := w.s.mo.FullMerge(kb, item.Value(), mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		// add the final merge to this batch
		batch.entries = badger.EntriesSet(batch.entries, kb, mergedVal)
	}

	return w.s.kv.BatchSet(batch.entries)
}

func (w *Writer) Close() error {
	w.s = nil
	return nil
}
