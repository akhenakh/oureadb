package badger

import (
	"bytes"

	"github.com/dgraph-io/badger"
)

type RangeIterator struct {
	iterator *badger.Iterator
	stop     []byte
}

func (i *RangeIterator) Seek(key []byte) {
	i.iterator.Seek(key)
}

func (i *RangeIterator) Next() {
	i.iterator.Next()
}

func (i *RangeIterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *RangeIterator) Key() []byte {
	return i.iterator.Item().Key()
}

func (i *RangeIterator) Value() []byte {
	return i.iterator.Item().Value()
}

func (i *RangeIterator) Valid() bool {
	if !i.iterator.Valid() {
		return false
	}

	if i.stop == nil || len(i.stop) == 0 {
		return true
	}

	if bytes.Compare(i.stop, i.iterator.Item().Key()) < 0 {
		return false
	}
	return true
}

func (i *RangeIterator) Close() error {
	i.iterator.Close()
	return nil
}
