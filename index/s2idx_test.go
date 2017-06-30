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
var ring = []float64{
	-71.2324869632721,
	46.79705550924151,
	-71.23148918151855,
	46.79630633487581,
	-71.22928977012634,
	46.795850949282105,
	-71.22731566429138,
	46.79614474688046,
	-71.22568488121033,
	46.79727585265817,
	-71.22525572776794,
	46.79828207612585,
	-71.22547030448912,
	46.79936907011552,
	-71.22538447380066,
	46.799706915125526,
	-71.22511625289917,
	46.79987583683501,
	-71.22511625289917,
	46.80001538045589,
	-71.22598528862,
	46.800786536044875,
	-71.2324869632721,
	46.79705550924151,
}

const (
	quebecCellID = 5528333301988720640
	s2Level      = 18
)

func TestGenericPolygonGeoStorage(t *testing.T) {
	s := openStore(t)
	defer cleanup(t, s)

	id := []byte("MYPOLY")

	idx := NewS2FlatIdx(s, []byte("TESTPREFIX"), s2Level)
	require.NotNil(t, idx)

	geo := &geodata.GeoData{
		Geometry: &geodata.Geometry{
			Coordinates: ring,
			Type:        geodata.Geometry_POLYGON,
		},
	}

	// indexing the polygon
	err := idx.GeoIndex(geo, id)
	require.NoError(t, err)

	// querying back the 1st point to find the cell
	c := s2.CellIDFromLatLng(s2.LatLngFromDegrees(geo.Geometry.Coordinates[1], geo.Geometry.Coordinates[0]))
	c = c.Parent(s2Level)
	res, err := idx.GeoIdsAtCell(c)
	require.NoError(t, err)
	require.Len(t, res, 1)
	t.Log("inserted", c)
}

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

func TestGenericPolygonGeoCovering(t *testing.T) {
	s, _ := null.New(nil, nil)
	defer s.Close()
	const s2Level = 18
	idx := NewS2FlatIdx(s, []byte("TEST"), s2Level)
	require.NotNil(t, idx)

	geo := &geodata.GeoData{
		Geometry: &geodata.Geometry{
			Coordinates: ring,
			Type:        geodata.Geometry_POLYGON,
		},
	}

	// covering the polygon
	cu, err := idx.Covering(geo)
	require.NoError(t, err)
	require.Len(t, cu, 188)
	t.Log(cu)
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
