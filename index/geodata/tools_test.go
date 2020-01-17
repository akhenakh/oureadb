package geodata

import (
	"encoding/json"
	"testing"

	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

var (
	quebec = []float64{-71.22759461402893, 46.79841427927054}
)

const (
	pointGeoJSON      = `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[-71.22759461402893, 46.79841427927054]}}]}`
	polygonGeoJSON    = `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-71.2324869632721,46.79705550924151],[-71.23148918151855,46.79630633487581],[-71.22928977012634,46.795850949282105],[-71.22731566429138,46.79614474688046],[-71.22568488121033,46.79727585265817],[-71.22525572776794,46.79828207612585],[-71.22547030448912,46.79936907011552],[-71.22538447380066,46.799706915125526],[-71.22511625289917,46.79987583683501],[-71.22511625289917,46.80001538045589],[-71.22598528862,46.800786536044875],[-71.2324869632721,46.79705550924151]]]}}]}`
	lineStringGeoJSON = `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"LineString","coordinates":[[-71.26419067382812,46.83735599002144],[-71.25045776367188,46.84328581149685],[-71.23689651489258,46.849156277107134],[-71.22685432434082,46.8535587053004]]}}]}`
)

func TestGeoJSONFeatureToGeoData(t *testing.T) {
	gd := &GeoData{}
	var fc geojson.FeatureCollection

	// Testing points
	err := json.Unmarshal([]byte(pointGeoJSON), &fc)
	require.NoError(t, err)

	f := fc.Features[0]
	err = GeoJSONFeatureToGeoData(f, gd)
	require.NoError(t, err)

	require.Equal(t, gd.Geometry.Type, Geometry_POINT)
	require.Equal(t, gd.Geometry.Coordinates[0], quebec[0])
	require.Equal(t, gd.Geometry.Coordinates[1], quebec[1])

	geop, err := GeoDataToGeom(gd)
	require.NoError(t, err)
	p, ok := geop.(*geom.Point)
	require.True(t, ok)
	require.NotNil(t, p)
	require.EqualValues(t, p.FlatCoords(), quebec)

	err = json.Unmarshal([]byte(polygonGeoJSON), &fc)
	require.NoError(t, err)

	f = fc.Features[0]
	err = GeoJSONFeatureToGeoData(f, gd)
	require.NoError(t, err)

	require.Equal(t, gd.Geometry.Type, Geometry_POLYGON)

	geopoly, err := GeoDataToGeom(gd)
	require.NoError(t, err)
	poly, ok := geopoly.(*geom.Polygon)
	require.True(t, ok)
	require.NotNil(t, p)
	require.EqualValues(t, poly.FlatCoords(), gd.Geometry.Coordinates)

	// Test geop is inside geopoly
	require.True(t, geopoly.Bounds().OverlapsPoint(geom.XY, geop.FlatCoords()))

	// Testing lines
	err = json.Unmarshal([]byte(lineStringGeoJSON), &fc)
	require.NoError(t, err)

	f = fc.Features[0]
	err = GeoJSONFeatureToGeoData(f, gd)
	require.NoError(t, err)

	require.Equal(t, gd.Geometry.Type, Geometry_LINESTRING)

	geoline, err := GeoDataToGeom(gd)
	require.NoError(t, err)
	geoline, ok = geoline.(*geom.LineString)
	require.True(t, ok)
	require.NotNil(t, p)
	require.EqualValues(t, geoline.FlatCoords(), gd.Geometry.Coordinates)
}

func TestLineCover(t *testing.T) {
	gd := &GeoData{}
	var fc geojson.FeatureCollection

	// Testing lines
	err := json.Unmarshal([]byte(lineStringGeoJSON), &fc)
	require.NoError(t, err)

	f := fc.Features[0]
	err = GeoJSONFeatureToGeoData(f, gd)
	require.NoError(t, err)

	coverer := &s2.RegionCoverer{MinLevel: 9}
	cu, err := gd.Cover(coverer)
	require.NoError(t, err)

	require.Len(t, cu, 2)
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

func TestBadLoop(t *testing.T) {
	s2pts := make([]s2.Point, 4)
	s2pts[0] = s2.PointFromLatLng(s2.LatLngFromDegrees(48.846457859383435, 2.4001533999884828))
	s2pts[1] = s2.PointFromLatLng(s2.LatLngFromDegrees(48.84650728362274, 2.4002499620546587))
	s2pts[2] = s2.PointFromLatLng(s2.LatLngFromDegrees(48.8453952264004, 2.401886152667487))
	s2pts[3] = s2.PointFromLatLng(s2.LatLngFromDegrees(48.84533874029373, 2.4018003197157896))

	if s2.RobustSign(s2pts[0], s2pts[1], s2pts[2]) == s2.Clockwise {
		t.Log("NOT CCW reversing")
		// reverse the slice
		for i := len(s2pts)/2 - 1; i >= 0; i-- {
			opp := len(s2pts) - 1 - i
			s2pts[i], s2pts[opp] = s2pts[opp], s2pts[i]
		}
	}

	for _, p := range s2pts {
		t.Log(s2.LatLngFromPoint(p).Lat.Degrees(), s2.LatLngFromPoint(p).Lng.Degrees())
	}
	l := s2.LoopFromPoints(s2pts)
	require.False(t, l.IsEmpty() || l.IsFull())
	rc := &s2.RegionCoverer{MinLevel: 18, MaxLevel: 18}
	covering := rc.Covering(l)
	t.Log(l, covering)
	require.Len(t, covering, 11)
}

func Test3PLoop(t *testing.T) {
	s2pts := make([]s2.Point, 3)
	s2pts[0] = s2.PointFromLatLng(s2.LatLngFromDegrees(53.51249996891279, -113.53350000023157))
	s2pts[1] = s2.PointFromLatLng(s2.LatLngFromDegrees(53.56133333693556, -113.5860000459106))
	s2pts[2] = s2.PointFromLatLng(s2.LatLngFromDegrees(53.5543333277993, -113.4728333483211))

	l := s2.LoopFromPoints(s2pts)
	t.Log(l)
	rc := &s2.RegionCoverer{MinLevel: 1, MaxLevel: 30, MaxCells: 32}
	if s2.RobustSign(s2pts[0], s2pts[1], s2pts[2]) == s2.Clockwise {
		t.Log("NOT CCW")
		s2pts[0], s2pts[1] = s2pts[1], s2pts[0]
	}
	l = s2.LoopFromPoints(s2pts)
	t.Log(l)
	covering := rc.Covering(l)
	t.Log(covering)
	require.True(t, len(covering) > 0)
}

func Test4PLoop(t *testing.T) {
	//[47.61649997100236 -122.20166669972009] [47.63100000228975 -122.18449997736279] [47.64100002912518 -122.1180000087143] [47.629666669247634 -122.03883334651307]
	s2pts := make([]s2.Point, 4)
	s2pts[0] = s2.PointFromLatLng(s2.LatLngFromDegrees(47.61649997100236, -122.20166669972009))
	s2pts[1] = s2.PointFromLatLng(s2.LatLngFromDegrees(47.63100000228975, -122.18449997736279))
	s2pts[2] = s2.PointFromLatLng(s2.LatLngFromDegrees(47.64100002912518, -122.1180000087143))
	s2pts[3] = s2.PointFromLatLng(s2.LatLngFromDegrees(47.629666669247634, -122.03883334651307))
	l := s2.LoopFromPoints(s2pts)
	t.Log(l)
	rc := &s2.RegionCoverer{MinLevel: 1, MaxLevel: 30, MaxCells: 32}
	if s2.RobustSign(s2pts[0], s2pts[1], s2pts[2]) != s2.CounterClockwise {
		t.Log("NOT CCW")
		s2pts[0], s2pts[1] = s2pts[1], s2pts[0]
	}
	l = s2.LoopFromPoints(s2pts)
	require.False(t, l.IsEmpty() || l.IsFull())
	t.Log(l)
	covering := rc.Covering(l)
	t.Log(covering)
	require.True(t, len(covering) > 0)
}
