package index

import (
	"testing"

	"bytes"
	"encoding/binary"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
)

func TestPointGeoStorage(t *testing.T) {
	s := openStore(t)
	defer cleanup(t, s)

	id := []byte("APOINT")

	idx := NewS2PointIdx(s, []byte("MYPREFIX"))
	require.NotNil(t, idx)

	k, err := idx.PointIndex(quebec[1], quebec[0], id)
	require.NoError(t, err)
	buf := bytes.NewBuffer(k[len(idx.prefix):])
	var c s2.CellID
	err = binary.Read(buf, binary.BigEndian, &c)
	require.NoError(t, err)
	t.Log(c.ToToken())

	res, err := idx.GeoIdsAtCell(c.Parent(10))
	require.NoError(t, err)
	require.Len(t, res, 1)
	t.Log("inserted", c)

	geo := &geodata.GeoData{
		Geometry: &geodata.Geometry{
			Coordinates: quebec,
			Type:        geodata.Geometry_POINT,
		},
	}

	// indexing the point
	k, err = idx.GeoPointIndex(geo, id)
	require.NoError(t, err)

	buf = bytes.NewBuffer(k[len(idx.prefix):])
	err = binary.Read(buf, binary.BigEndian, &c)
	require.NoError(t, err)

	t.Log(c.ToToken())

	// querying back the point
	cs := s2.CellIDFromLatLng(s2.LatLngFromDegrees(geo.Geometry.Coordinates[1], geo.Geometry.Coordinates[0]))

	res, err = idx.GeoIdsAtCell(cs.Parent(10))
	require.NoError(t, err)
	require.Len(t, res, 1)
	t.Log("inserted", c)

	// querying radius
	res, err = idx.GeoIdsRadiusQuery(46.809, -71.212, 5000)
	require.NoError(t, err)
	require.Len(t, res, 1)

	// querying rect
	res, err = idx.GeoIdsRectQuery(47.042521787558435, -71.03279113769531, 46.6451938027548, -71.47705078125)
	require.NoError(t, err)
	require.Len(t, res, 1)
}

func TestPointGeoCovering(t *testing.T) {
	s := openStore(t)
	defer cleanup(t, s)
	prefix := []byte("TEST")

	idx := NewS2PointIdx(s, prefix)
	require.NotNil(t, idx)

	fakeID := []byte("32")
	// indexing the point
	k, err := idx.PointIndex(quebec[1], quebec[0], fakeID)
	require.NoError(t, err)
	require.Len(t, k, len(prefix)+8+len(fakeID))

	// should find one cell in the cover union of level 18
	quebecCellL30ID := s2.CellID(5528333301978098741)
	buf := bytes.NewBuffer(k[len(idx.prefix):])
	var c s2.CellID
	err = binary.Read(buf, binary.BigEndian, &c)
	t.Log(c.ToToken())
	require.EqualValues(t, c, quebecCellL30ID)
}
