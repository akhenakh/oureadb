package index

import (
	"testing"

	"time"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/stretchr/testify/require"
)

// lng, lat
var paris = []float64{2.3489123582839966, 48.8580058267693}

func TestPointGeoTimeStorage(t *testing.T) {
	s := openStore(t)
	defer cleanup(t, s)

	id := []byte("MYPOINTID")

	idx := NewS2FlatTimeIdx(s, []byte("TESTPREFIX"), s2Level)
	require.NotNil(t, idx)

	geo := &geodata.GeoData{
		Geometry: &geodata.Geometry{
			Coordinates: paris,
			Type:        geodata.Geometry_POINT,
		},
	}

	now := time.Now()

	// indexing the point
	err := idx.GeoTimeIndex(geo, now, id)
	require.NoError(t, err)

	// Querying all from future to past
	res, err := idx.GeoTimeIdsRadiusQuery(MaxGeoTime, MinGeoTime, 48.850, 2.348, 2000)
	require.NoError(t, err)
	require.Len(t, res, 1)

	// Querying only in the past
	past := now.Add(-2 * time.Hour)
	res, err = idx.GeoTimeIdsRadiusQuery(past, MinGeoTime, 48.850, 2.348, 2000)
	require.NoError(t, err)
	require.Len(t, res, 0)
}
