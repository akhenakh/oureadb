package index

import (
	"testing"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/index/store/null"
	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
)

// lng, lat
var quebec = []float64{-71.21266275644302, 46.80960262934726}

const (
	quebecCellID = 5528333301988720640
	s2Level      = 14
)

func TestGenericPointGeoStorage(t *testing.T) {
	s := openStore(t)
	defer cleanup(t, s)

	id := []byte("MYPOINTID")

	idx := NewS2FlatIdx(s, []byte("TESTPREFIX"), s2Level)
	require.NotNil(t, idx)

	geo := &geodata.GeoData{
		Geometry: &geodata.Geometry{
			Coordinates: quebec,
			Type:        geodata.Geometry_POINT,
		},
	}

	// indexing the point
	err := idx.GeoIndex(geo, id)
	require.NoError(t, err)

	// querying back the point
	c := s2.CellIDFromLatLng(s2.LatLngFromDegrees(geo.Geometry.Coordinates[1], geo.Geometry.Coordinates[0]))
	t.Log("inserted", c)

	res, err := idx.GeoIdsAtCell(c)

	// the cell is not at the right level
	require.Error(t, err)
	require.Len(t, res, 0)

	// querying at the right level
	c = c.Parent(s2Level)
	res, err = idx.GeoIdsAtCell(c)
	require.NoError(t, err)
	require.Len(t, res, 1)
	t.Log("inserted", c)

	// querying radius
	res, err = idx.GeoIdsRadiusQuery(46.809, -71.212, 5000)
	require.NoError(t, err)
	require.Len(t, res, 1)
}

func TestGenericPointGeoCovering(t *testing.T) {
	s, _ := null.New(nil, nil)
	defer s.Close()
	const s2Level = 18
	idx := NewS2FlatIdx(s, []byte("TEST"), s2Level)
	require.NotNil(t, idx)

	geo := &geodata.GeoData{
		Geometry: &geodata.Geometry{
			Coordinates: quebec,
			Type:        geodata.Geometry_POINT,
		},
	}

	// indexing the point
	cu, err := idx.Covering(geo)
	require.NoError(t, err)
	require.Len(t, cu, 1)
	t.Log(cu)

	// should find one cell in the cover union of level 18
	quebecCellID := s2.CellID(quebecCellID)
	require.EqualValues(t, cu[0], quebecCellID)
}

func openStore(t *testing.T) store.KVStore {
	rv, err := gtreap.New(nil, map[string]interface{}{
		"path": "",
	})
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

func cleanup(t *testing.T, s store.KVStore) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}
}
