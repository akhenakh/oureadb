package index

import (
	"bytes"
	"encoding/binary"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/blevesearch/bleve/index/store"
	"github.com/golang/geo/s2"
	"github.com/pkg/errors"
)

// S2FlatIdx a flat S2 region cover index
type S2FlatIdx struct {
	// s2 level to index
	level int

	// prefix or bucket for the keys
	prefix []byte

	store.KVStore
}

// NewS2FlatIdx returns a new indexer
func NewS2FlatIdx(s store.KVStore, prefix []byte, level int) *S2FlatIdx {
	return &S2FlatIdx{
		KVStore: s,
		prefix:  prefix,
		level:   level,
	}
}

// GeoIndex is geo indexing the geo data
// it's not storing GeoData itself but only the geo index of the cover
// k is the key referring to the GeoData stored somewhere else
func (idx *S2FlatIdx) GeoIndex(gd *geodata.GeoData, k []byte) error {
	cu, err := idx.Covering(gd)
	if err != nil {
		return errors.Wrap(err, "generating cover failed")
	}

	kv, err := idx.KVStore.Writer()
	if err != nil {
		return err
	}

	batch := kv.NewBatch()
	defer batch.Close()

	// For each cell we store
	// a key prefix+cellid+id -> no values
	for _, c := range cu {
		k := make([]byte, len(idx.prefix))
		copy(k, idx.prefix)
		k = append(k, itob(uint64(c))...)
		k = append(k, k...)
		batch.Set(k, nil)
	}

	return kv.ExecuteBatch(batch)
}

// GeoIdsAtCell returns all GeoData keys contained in the cell
func (idx *S2FlatIdx) GeoIdsAtCell(c s2.CellID) ([][]byte, error) {
	if c.Level() != idx.level {
		return nil, errors.New("requested a cellID with a different level than the index")
	}

	// add the prefix to the queried key
	k := make([]byte, len(idx.prefix))
	copy(k, idx.prefix)
	k = append(k, itob(uint64(c))...)

	var res [][]byte

	kv, err := idx.Reader()
	if err != nil {
		return nil, err
	}

	// iterate over the cell
	iter := kv.PrefixIterator(k)
	for {
		kid, _, ok := iter.Current()
		if !ok {
			break
		}
		_, id, err := idx.geoKeyToValues(kid)
		if err != nil {
			return nil, errors.Wrap(err, "leveldb read back failed")
		}

		res = append(res, id)
		iter.Next()
	}

	return res, nil
}

// Covering is generating the cover of a GeoData
func (idx *S2FlatIdx) Covering(gd *geodata.GeoData) (s2.CellUnion, error) {
	coverer := &s2.RegionCoverer{MinLevel: idx.level, MaxCells: idx.level}
	return GeoDataToFlatCellUnion(gd, coverer)
}

func (idx *S2FlatIdx) geoKeyToValues(k []byte) (c s2.CellID, id []byte, err error) {
	// prefix+cellid+id
	if len(k) <= len(idx.prefix)+8 {
		return c, id, errors.New("invalid key")
	}
	buf := bytes.NewBuffer(k[len(idx.prefix):])
	err = binary.Read(buf, binary.BigEndian, &c)
	if err != nil {
		return c, id, errors.Wrap(err, "read back cell key failed")
	}

	id = k[len(idx.prefix)+8:]
	return
}
