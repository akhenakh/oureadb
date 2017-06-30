package tools

import (
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/geojson"
)

// GeoJSONCellUnion helpers to display s2 cells on maps with GeoJSON
type GeoJSONCellUnion s2.CellUnion

// ToGeoJSON export a cell union into its GeoJSON representation
func (cu GeoJSONCellUnion) ToGeoJSON() []byte {
	fc := geojson.FeatureCollection{}
	for _, cid := range cu {
		f := &geojson.Feature{}
		f.Properties = make(map[string]interface{})
		f.Properties["id"] = cid.ToToken()
		c := s2.CellFromCellID(cid)
		coords := make([]float64, 4*2)
		for i := 0; i < 4; i++ {
			p := c.Vertex(i)
			ll := s2.LatLngFromPoint(p)
			coords[i*2] = ll.Lng.Degrees()
			coords[i*2+1] = ll.Lat.Degrees()
		}
		ng := geom.NewPolygonFlat(geom.XY, coords, []int{4})
		f.Geometry = ng
		fc.Features = append(fc.Features, f)
	}
	return nil
}

// ToTokens a cell union to a token list
func (cu GeoJSONCellUnion) ToTokens() []string {
	res := make([]string, len(cu))

	for i, c := range cu {
		res[i] = c.ToToken()
	}
	return res
}
