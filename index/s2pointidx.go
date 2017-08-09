package index

import (
	"bytes"
	"encoding/binary"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/akhenakh/oureadb/store"
	"github.com/golang/geo/s2"
	"github.com/pkg/errors"
)

// S2PointIdx is an s2 point index
type S2PointIdx struct {
	// prefix for the keys
	prefix []byte

	store.KVStore
}

// NewS2PointIdx returns a new indexer
func NewS2PointIdx(s store.KVStore, prefix []byte) *S2PointIdx {
	return &S2PointIdx{
		KVStore: s,
		prefix:  prefix,
	}
}

// PointIndex is geo indexing a point
// it's not storing GeoData itself but only the geo cell l30 of the point
// id is the key referring to the GeoData stored somewhere else
func (idx *S2PointIdx) PointIndex(lat, lng float64, id GeoID) ([]byte, error) {
	kv, err := idx.KVStore.Writer()
	if err != nil {
		return nil, err
	}

	batch := kv.NewBatch()
	defer batch.Close()

	k := idx.PointKey(lat, lng, id)
	batch.Set(k, nil)

	return k, kv.ExecuteBatch(batch)
}

// PointKey is returning the key generated for a position + id
func (idx *S2PointIdx) PointKey(lat, lng float64, id GeoID) []byte {
	c := s2.CellIDFromLatLng(s2.LatLngFromDegrees(lat, lng))
	// a key prefix+cellid+id -> no values
	k := make([]byte, len(idx.prefix))
	copy(k, idx.prefix)

	// avoid any retain by copying id
	cid := append([]byte(nil), id...)

	k = append(k, itob(uint64(c))...)
	k = append(k, []byte(cid)...)
	return k
}

// PointKey is returning the key generated for a geopoint + id
func (idx *S2PointIdx) GeoPointKey(gd *geodata.GeoData, id GeoID) ([]byte, error) {
	if gd.Geometry.Type != geodata.Geometry_POINT {
		return nil, errors.New("only points are supported")
	}

	return idx.PointKey(gd.Geometry.Coordinates[1], gd.Geometry.Coordinates[0], id), nil
}

// GeoPointIndex is geo indexing the geo data point
// it's not storing GeoData itself but only the geo cell l30 of the point
// id is the key referring to the GeoData stored somewhere else
func (idx *S2PointIdx) GeoPointIndex(gd *geodata.GeoData, id GeoID) ([]byte, error) {
	if gd.Geometry.Type != geodata.Geometry_POINT {
		return nil, errors.New("only points are supported")
	}

	return idx.PointIndex(gd.Geometry.Coordinates[1], gd.Geometry.Coordinates[0], id)
}

// GeoIdsAtCell returns all GeoData keys contained in the cell
func (idx *S2PointIdx) GeoIdsAtCell(c s2.CellID) ([]GeoID, error) {
	// add the prefix to the queried key
	start := make([]byte, len(idx.prefix))
	copy(start, idx.prefix)
	start = append(start, itob(uint64(c.RangeMin()))...)
	stop := make([]byte, len(idx.prefix))
	copy(stop, idx.prefix)
	stop = append(stop, itob(uint64(c.RangeMax()))...)

	var res []GeoID

	kv, err := idx.Reader()
	if err != nil {
		return nil, err
	}

	// iterate entries with cell's prefix
	iter := kv.RangeIterator(start, stop)
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
func (idx *S2PointIdx) GeoIdsAtCells(cells []s2.CellID) ([]GeoID, error) {
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
func (idx *S2PointIdx) GeoIdsRadiusQuery(lat, lng, radius float64) ([]GeoID, error) {
	center := s2.PointFromLatLng(s2.LatLngFromDegrees(lat, lng))
	cap := s2.CapFromCenterArea(center, s2RadialAreaMeters(radius))
	coverer := &s2.RegionCoverer{MaxLevel: 14, MaxCells: 8}
	cu := coverer.Covering(cap)

	kv, err := idx.Reader()
	defer kv.Close()
	if err != nil {
		return nil, err
	}

	var res []GeoID

	// we range over cover to lookup for points (cell l30) and filter them in place
	for _, coverCell := range cu {
		start := make([]byte, len(idx.prefix))
		copy(start, idx.prefix)
		start = append(start, itob(uint64(coverCell.RangeMin()))...)
		stop := make([]byte, len(idx.prefix))
		copy(stop, idx.prefix)
		stop = append(stop, itob(uint64(coverCell.RangeMax()))...)

		// iterate entries with cell's prefix
		iter := kv.RangeIterator(start, stop)

		for {
			kid, _, ok := iter.Current()
			if !ok {
				break
			}

			kid = append([]byte(nil), kid...)

			c, id, err := idx.keyToValues(kid)
			if err != nil {
				return nil, errors.Wrap(err, "read back failed key from db")
			}

			// filter in place
			if cap.ContainsPoint(c.Point()) {
				res = append(res, id)
			}

			iter.Next()
		}
		iter.Close()
	}
	return res, nil
}

// GeoTimeIdsRectQuery query over rect ur upper right bl bottom left
func (idx *S2PointIdx) GeoIdsRectQuery(urlat, urlng, bllat, bllng float64) ([]GeoID, error) {
	rect := s2.RectFromLatLng(s2.LatLngFromDegrees(bllat, bllng))
	rect = rect.AddPoint(s2.LatLngFromDegrees(urlat, urlng))
	coverer := &s2.RegionCoverer{MaxLevel: 14, MaxCells: 8}
	cu := coverer.Covering(rect)

	var res []GeoID

	kv, err := idx.Reader()
	defer kv.Close()
	if err != nil {
		return nil, err
	}

	// we range over cover to lookup for points (cell l30) and filter them in place
	for _, coverCell := range cu {
		start := make([]byte, len(idx.prefix))
		copy(start, idx.prefix)
		start = append(start, itob(uint64(coverCell.RangeMin()))...)
		stop := make([]byte, len(idx.prefix))
		copy(stop, idx.prefix)
		stop = append(stop, itob(uint64(coverCell.RangeMax()))...)

		// iterate entries with cell's prefix
		iter := kv.RangeIterator(start, stop)
		for {
			kid, _, ok := iter.Current()
			if !ok {
				break
			}
			kid = append([]byte(nil), kid...)

			c, id, err := idx.keyToValues(kid)
			if err != nil {
				return nil, errors.Wrap(err, "read back failed key from db")
			}

			// filter in place
			//TODO: find a CointainsPoint interface to factorize code with radius
			if rect.ContainsPoint(c.Point()) {
				res = append(res, id)
			}

			iter.Next()
		}
		iter.Close()
	}
	return res, nil
}

func (idx *S2PointIdx) keyToValues(k []byte) (c s2.CellID, id GeoID, err error) {
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
