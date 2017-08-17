package badger

import (
	"github.com/akhenakh/oureadb/store"
	"github.com/dgraph-io/badger"
)

type Reader struct {
	kv      *badger.KV
	itrOpts *badger.IteratorOptions
}

func (r *Reader) Get(k []byte) ([]byte, error) {
	item := &badger.KVItem{}
	err := r.kv.Get(k, item)
	if err != nil {
		return nil, err
	}

	vs := item.Value()
	v := make([]byte, len(vs))
	copy(v, vs)

	return v, nil
}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return store.MultiGet(r, keys)
}

func (r *Reader) PrefixIterator(k []byte) store.KVIterator {
	rv := PrefixIterator{
		iterator: r.kv.NewIterator(*r.itrOpts),
		prefix:   k[:],
	}
	rv.iterator.Seek(k)
	return &rv
}

func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	rv := RangeIterator{
		iterator: r.kv.NewIterator(*r.itrOpts),
		stop:     end[:],
	}
	rv.iterator.Seek(start)
	return &rv
}

func (r *Reader) Close() error {
	return nil
}
