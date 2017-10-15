package s2tools

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom/encoding/geojson"
)

const parisGeoJSON = `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[2.3212051391601562,48.90016083346632],[2.2896194458007812,48.887746278609676],[2.2810363769531246,48.87803820654905],[2.253570556640625,48.840090929598006],[2.2587203979492188,48.83466754148594],[2.2721099853515625,48.83511951292218],[2.2930526733398438,48.82720942407081],[2.3342514038085938,48.81703747481909],[2.3459243774414062,48.81635927146624],[2.35382080078125,48.81794173168324],[2.357940673828125,48.815228912154815],[2.3823165893554688,48.823819003701075],[2.4111557006835938,48.833989576686],[2.4145889282226562,48.85342092943525],[2.4084091186523438,48.87871557505334],[2.39776611328125,48.88390842884749],[2.3912429809570312,48.90016083346632],[2.3569107055664062,48.90128927649513],[2.3212051391601562,48.90016083346632]]]}}]}`

func TestGeoJSONHandler(t *testing.T) {
	req := httptest.NewRequest("POST", "http://example.com/9/9/0", strings.NewReader(parisGeoJSON))
	w := httptest.NewRecorder()

	m := mux.NewRouter()
	m.HandleFunc("/{min_level:[0-9]+}/{max_level:[0-9]+}/{max_cells:[0-9]+}", GeoJSONToCellHandler)
	m.ServeHTTP(w, req)

	GeoJSONToCellHandler(w, req)

	resp := w.Result()
	require.Equal(t, 200, resp.StatusCode)

	var fc geojson.FeatureCollection
	decoder := json.NewDecoder(resp.Body)
	err := decoder.Decode(&fc)
	require.NoError(t, err)
	req.Body.Close()

	require.Len(t, fc.Features, 4)

	// expected cells are
	isValid := func(token string) bool {
		switch token {
		case
			"47e664",
			"47e66c",
			"47e674",
			"47e67c":
			return true
		}
		return false
	}

	for _, f := range fc.Features {
		v, ok := f.Properties["id"]
		require.True(t, ok)
		require.True(t, isValid(v.(string)))
	}

}
