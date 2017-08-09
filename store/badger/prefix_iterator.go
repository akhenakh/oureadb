package badger

import "github.com/dgraph-io/badger"

type PrefixIterator struct {
	iterator *badger.Iterator
	prefix   []byte
}

func (i *PrefixIterator) Seek(key []byte) {
	i.iterator.Seek(key)
}

func (i *PrefixIterator) Next() {
	i.iterator.Next()
}

func (i *PrefixIterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *PrefixIterator) Key() []byte {
	return i.iterator.Item().Key()
}

func (i *PrefixIterator) Value() []byte {
	return i.iterator.Item().Value()
}

func (i *PrefixIterator) Valid() bool {
	return i.iterator.ValidForPrefix(i.prefix)
}

func (i *PrefixIterator) Close() error {
	i.iterator.Close()
	return nil
}
