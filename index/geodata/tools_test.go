package geodata

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom/encoding/geojson"
)

var quebec = []float64{-71.21266275644302, 46.80960262934726}

const (
	pointGeoJSON = `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[-71.21266275644302,46.80960262934726]}}]}`
)

func TestGeoJSONFeatureToGeoData(t *testing.T) {
	gd := &GeoData{}
	var fc geojson.FeatureCollection

	err := json.Unmarshal([]byte(pointGeoJSON), &fc)
	require.NoError(t, err)

	f := fc.Features[0]
	err = GeoJSONFeatureToGeoData(f, gd)
	require.NoError(t, err)

	require.Equal(t, gd.Geometry.Type, Geometry_POINT)
	require.Equal(t, gd.Geometry.Coordinates[0], quebec[0])
	require.Equal(t, gd.Geometry.Coordinates[1], quebec[1])
}

func TestPointsToGeoJSONPolyLines(t *testing.T) {
	geo0 := &GeoData{
		Geometry: &Geometry{
			Coordinates: []float64{2.3489123582839966, 48.85800406207049},
			Type:        Geometry_POINT,
		},
	}

	geo1 := &GeoData{
		Geometry: &Geometry{
			Coordinates: []float64{2.3489123582839964, 48.85800406207042},
			Type:        Geometry_POINT,
		},
	}

	gs := []*GeoData{geo0, geo1}

	js, err := PointsToGeoJSONPolyLines(gs)
	require.NoError(t, err)
	t.Log(string(js))
}
