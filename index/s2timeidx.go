package index

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/akhenakh/oureadb/store"
	"github.com/golang/geo/s2"
	"github.com/pkg/errors"
)

// S2FlatTimeIdx a flat S2 region cover time to index points & polygons
type S2FlatTimeIdx struct {
	// s2 level to index
	level int

	// prefix or bucket for the keys
	prefix []byte

	store.KVStore
}

// NewS2FlatTimeIdx returns a new indexer
func NewS2FlatTimeIdx(s store.KVStore, prefix []byte, level int) *S2FlatTimeIdx {
	return &S2FlatTimeIdx{
		KVStore: s,
		prefix:  prefix,
		level:   level,
	}
}

// GeoTimeIndex is indexing the data by time and geo position
// it's not storing GeoData itself but only the geo index of the cover
// id is the key referring to the GeoData stored somewhere else
// it's meant to index geographical points or polygon attached to a time event
// could be a user position or an effect zone
// t is the time when the event end (can be in the future)
func (idx *S2FlatTimeIdx) GeoTimeIndex(gd *geodata.GeoData, t time.Time, id GeoID) error {
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

	// prefix + s2 cell uint64 (constant level) + t uint64 + k => nil
	for _, c := range cu {
		k := idx.valuesToKey(c, t, id)

		//TODO: optional set value is the full s2 cellid in case of a point
		batch.Set(k, nil)
	}

	return kv.ExecuteBatch(batch)
}

// Covering is generating the cover of a GeoData
func (idx *S2FlatTimeIdx) Covering(gd *geodata.GeoData) (s2.CellUnion, error) {
	coverer := &s2.RegionCoverer{MinLevel: idx.level, MaxCells: idx.level}
	return geodata.GeoDataToFlatCellUnion(gd, coverer)
}

// GeoTimeIdsRadiusQuery query over radius in meters within time range
// scan from future to past
func (idx *S2FlatTimeIdx) GeoTimeIdsRadiusQuery(from time.Time, to time.Time, lat, lng float64, radius float64) ([]GeoID, error) {
	center := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
	cap := s2.CapFromCenterArea(center, s2RadialAreaMeters(radius))
	coverer := &s2.RegionCoverer{MinLevel: idx.level, MaxLevel: idx.level}
	cu := coverer.Covering(cap)
	return idx.GeoTimeIdsAtCells(cu, from, to)
}

// GeoTimeIdsRectQuery query over rect ur upper right bl bottom left
// scan from future to past
func (idx *S2FlatTimeIdx) GeoTimeIdsRectQuery(from time.Time, to time.Time, urlat, urlng, bllat, bllng float64) ([]GeoID, error) {
	rect := s2.RectFromLatLng(s2.LatLngFromDegrees(bllat, bllng))
	rect = rect.AddPoint(s2.LatLngFromDegrees(urlat, urlng))
	coverer := &s2.RegionCoverer{MinLevel: idx.level, MaxLevel: idx.level}
	cu := coverer.Covering(rect)
	return idx.GeoTimeIdsAtCells(cu, from, to)
}

// GeoTimeIdsAtCells returns all GeoData keys contained in the cells within time range, without duplicates
func (idx *S2FlatTimeIdx) GeoTimeIdsAtCells(cells []s2.CellID, from time.Time, to time.Time) ([]GeoID, error) {
	m := make(map[string]struct{})

	for _, c := range cells {
		ids, err := idx.GeoTimeIdsAtCell(c, from, to)
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

// GeoTimeIdsAtCell returns all GeoData keys contained in the cell from time from  to time to
func (idx *S2FlatTimeIdx) GeoTimeIdsAtCell(c s2.CellID, from time.Time, to time.Time) ([]GeoID, error) {
	if c.Level() != idx.level {
		return nil, errors.New("requested a cellID with a different level than the index")
	}

	startKey := idx.timePrefixKey(c, from)
	stopKey := idx.timePrefixKey(c, to)

	var res []GeoID

	kv, err := idx.Reader()
	if err != nil {
		return nil, err
	}

	// iterate entries with cell's prefix
	iter := kv.RangeIterator(startKey, stopKey)
	for {
		kid, _, ok := iter.Current()
		if !ok {
			break
		}
		_, _, id, err := idx.keyToValues(kid)
		if err != nil {
			return nil, errors.Wrap(err, "read back failed key from db")
		}

		res = append(res, id)
		iter.Next()
	}

	return res, nil
}

// valuesToKey returns the key for a triplets cell/time/id
func (idx *S2FlatTimeIdx) valuesToKey(c s2.CellID, t time.Time, id GeoID) []byte {
	k := idx.timePrefixKey(c, t)
	k = append(k, []byte(id)...)
	return k
}

// timePrefixKey returns the key prefix for a cell and a time
func (idx *S2FlatTimeIdx) timePrefixKey(c s2.CellID, t time.Time) []byte {
	k := make([]byte, len(idx.prefix))
	copy(k, idx.prefix)
	k = append(k, itob(uint64(c))...)
	// adding time, using reverse timestamp
	ts := int64tob(math.MaxInt64 - t.UnixNano())
	k = append(k, ts...)
	return k
}

func (idx *S2FlatTimeIdx) keyToValues(k []byte) (c s2.CellID, t time.Time, id GeoID, err error) {
	// prefix+cellid+ts+id
	if len(k) <= len(idx.prefix)+8+8 {
		return c, t, id, errors.New("invalid key")
	}
	buf := bytes.NewBuffer(k[len(idx.prefix):])

	// read back cell
	err = binary.Read(buf, binary.BigEndian, &c)
	if err != nil {
		return c, t, id, errors.Wrap(err, "read back cell key failed")
	}

	// read back ts
	var ts int64
	err = binary.Read(buf, binary.BigEndian, &ts)
	if err != nil {
		return c, t, id, errors.Wrap(err, "read back ts key failed")
	}

	// reverse ts back to ts
	t = time.Unix(0, math.MaxInt64-ts)

	id = k[len(idx.prefix)+8+8:]
	return
}
