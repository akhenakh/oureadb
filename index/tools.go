package index

import (
	"encoding/binary"
	"fmt"

	"github.com/akhenakh/oureadb/index/geodata"
	"github.com/golang/geo/s2"
	"github.com/golang/protobuf/ptypes/struct"
	"github.com/pkg/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

// GeoToGeoData update gd with geo data gathered from g
func GeoToGeoData(g geom.T, gd *geodata.GeoData) error {
	geo := &geodata.Geometry{}

	switch g := g.(type) {
	case *geom.Point:
		geo.Coordinates = g.Coords()
		geo.Type = geodata.Geometry_POINT
	//case *geom.MultiPolygon:
	//	geo.Type = geodata.Geometry_MULTIPOLYGON
	//	// TODO implement multipolygon
	//
	//case *geom.Polygon:
	//	geo.Type = geodata.Geometry_POLYGON
	//	// TODO implement polygon
	default:
		return errors.Errorf("unsupported geo type %T", g)
	}

	gd.Geometry = geo
	return nil
}

// PropertiesToGeoData update gd.Properties with the properties found in f
func PropertiesToGeoData(f *geojson.Feature, gd *geodata.GeoData) error {
	for k, vi := range f.Properties {
		switch tv := vi.(type) {
		case bool:
			gd.Properties[k] = &structpb.Value{Kind: &structpb.Value_BoolValue{BoolValue: tv}}
		case int:
			gd.Properties[k] = &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: float64(tv)}}
		case string:
			gd.Properties[k] = &structpb.Value{Kind: &structpb.Value_StringValue{StringValue: tv}}
		case float64:
			gd.Properties[k] = &structpb.Value{Kind: &structpb.Value_NumberValue{NumberValue: tv}}
		case nil:
			// pass
		default:
			return fmt.Errorf("GeoJSON property %s unsupported type %T", k, tv)
		}
	}
	return nil
}

// FeatureToGeoData fill gd with the GeoJSON data f
func FeatureToGeoData(f *geojson.Feature, gd *geodata.GeoData) error {
	err := PropertiesToGeoData(f, gd)
	if err != nil {
		return errors.Wrap(err, "while converting feature properties to GeoData")
	}

	err = GeoToGeoData(f.Geometry, gd)
	if err != nil {
		return errors.Wrap(err, "while converting feature to GeoData")
	}

	return nil
}

func GeoDataToFlatCellUnion(gd *geodata.GeoData, coverer *s2.RegionCoverer) (s2.CellUnion, error) {
	var cu s2.CellUnion
	switch gd.Geometry.Type {
	case geodata.Geometry_POINT:
		c := s2.CellIDFromLatLng(s2.LatLngFromDegrees(gd.Geometry.Coordinates[1], gd.Geometry.Coordinates[0]))
		cu = append(cu, c.Parent(coverer.MinLevel))
	case geodata.Geometry_POLYGON:
		if len(gd.Geometry.Coordinates) < 6 {
			return nil, errors.New("invalid polygons too few coordinates")
		}
		if len(gd.Geometry.Coordinates)%2 != 0 {
			return nil, errors.New("invalid polygons odd coordinates number")
		}
		l := LoopFenceFromCoordinates(gd.Geometry.Coordinates)
		if l.IsEmpty() || l.IsFull() || l.ContainsOrigin() {
			return nil, errors.New("invalid polygons")
		}

		cu = coverer.Covering(l)
	case geodata.Geometry_MULTIPOLYGON:
		for _, g := range gd.Geometry.Geometries {
			if len(g.Coordinates) < 6 {
				return nil, errors.New("invalid polygons too few coordinates")
			}
			if len(g.Coordinates)%2 != 0 {
				return nil, errors.New("invalid polygons odd coordinates number")
			}
			l := LoopFenceFromCoordinates(g.Coordinates)
			if l.IsEmpty() || l.IsFull() || l.ContainsOrigin() {
				return nil, errors.New("invalid polygons")
			}

			cu = append(cu, coverer.Covering(l.RectBound())...)
		}

	default:
		return nil, errors.New("unsupported data type")
	}

	return cu, nil
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
