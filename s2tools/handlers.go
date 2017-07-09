package s2tools

import (
	"net/http"
	"strings"

	"github.com/golang/geo/s2"
)

// S2CellQueryHandler returns a GeoJSON containing the cells passed in the query
// ?cells=TokenID,...
func S2CellQueryHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	sval := query.Get("cells")
	cells := strings.Split(sval, ",")
	if len(cells) == 0 {
		http.Error(w, "invalid parameters", 400)
		return
	}

	cu := make(s2.CellUnion, len(cells))

	for i, cs := range cells {
		c := s2.CellIDFromToken(cs)
		cu[i] = c
	}

	w.Header().Set("Content-Type", "application/json")

	w.Write(CellUnionToGeoJSON(cu))
}
