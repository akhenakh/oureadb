package tools

import "github.com/golang/geo/s2"

// GeoJSONCellUnion helpers to display s2 cells on maps with GeoJSON
type GeoJSONCellUnion s2.CellUnion

// ToGeoJSON export a cell union into its GeoJSON representation
func (cu *GeoJSONCellUnion) ToGeoJSON() []byte {
	//TODO: cell union to goejson
	return nil
}
