package index

import (
	"bytes"
	"encoding/binary"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/akhenakh/oureadb/store"
	"github.com/golang/geo/s2"
	"github.com/pkg/errors"
)

// S2FlatIdx a flat S2 region cover index
type S2FlatIdx struct {
	// s2 level to index
	level int

	// prefix for the keys
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
// id is the key referring to the GeoData stored somewhere else
func (idx *S2FlatIdx) GeoIndex(gd *geodata.GeoData, id GeoID) error {
	cu, err := idx.Covering(gd)
	if err != nil {
		return errors.Wrap(err, "generating cover failed")
	}

	// no cover for this geo object this is probably an error
	if len(cu) == 0 {
		return errors.New("geo object can't be indexed, empty cover")
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
		k = append(k, []byte(id)...)
		batch.Set(k, nil)
	}

	return kv.ExecuteBatch(batch)
}

// GeoIdsAtCell returns all GeoData keys contained in the cell
func (idx *S2FlatIdx) GeoIdsAtCell(c s2.CellID) ([]GeoID, error) {
	if c.Level() != idx.level {
		return nil, errors.New("requested a cellID with a different level than the index")
	}

	// add the prefix to the queried key
	k := make([]byte, len(idx.prefix))
	copy(k, idx.prefix)
	k = append(k, itob(uint64(c))...)

	var res []GeoID

	kv, err := idx.Reader()
	defer kv.Close()
	if err != nil {
		return nil, err
	}

	// iterate entries with cell's prefix
	iter := kv.PrefixIterator(k)
	defer iter.Close()
	for {
		kid, _, ok := iter.Current()
		if !ok {
			break
		}
		_, id, err := idx.keyToValues(kid)
		if err != nil {
			return nil, errors.Wrap(err, "read back failed key from db")
		}

		res = append(res, id)
		iter.Next()
	}

	return res, nil
}

// GeoIdsAtCells returns all GeoData keys contained in the cells, without duplicates
func (idx *S2FlatIdx) GeoIdsAtCells(cells []s2.CellID) ([]GeoID, error) {
	m := make(map[string]struct{})

	for _, c := range cells {
		ids, err := idx.GeoIdsAtCell(c)
		if err != nil {
			return nil, errors.Wrap(err, "fetching geo ids from cells failed")
		}
		for _, id := range ids {
			m[string(id)] = struct{}{}
		}
	}

	res := make([]GeoID, len(m))
	var i int
	for k := range m {
		res[i] = []byte(k)
		i++
	}

	return res, nil
}

// GeoIdsRadiusQuery returns the GeoID found in the index inside radius
// note you should check the returned GeoData is really inside/intersects the cap
func (idx *S2FlatIdx) GeoIdsRadiusQuery(lat, lng, radius float64) ([]GeoID, error) {
	center := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
	cap := s2.CapFromCenterArea(center, s2RadialAreaMeters(radius))
	coverer := &s2.RegionCoverer{MinLevel: idx.level, MaxLevel: idx.level}
	cu := coverer.Covering(cap)
	return idx.GeoIdsAtCells(cu)
}

// Covering is generating the cover of a GeoData
func (idx *S2FlatIdx) Covering(gd *geodata.GeoData) (s2.CellUnion, error) {
	coverer := &s2.RegionCoverer{MinLevel: idx.level, MaxCells: idx.level}
	return geodata.GeoDataToFlatCellUnion(gd, coverer)
}

func (idx *S2FlatIdx) keyToValues(k []byte) (c s2.CellID, id GeoID, err error) {
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
