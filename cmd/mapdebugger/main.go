package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/akhenakh/oureadb/s2tools"
	"github.com/gorilla/mux"
)

var (
	httpPort = flag.Int("httpPort", 8000, "http debug port to listen on")
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()

	r := mux.NewRouter()
	r.HandleFunc("/api/s2cells", s2tools.S2CellQueryHandler)
	r.HandleFunc("/api/geojson/{min_level:[0-9]+}/{max_level:[0-9]+}/{max_cells:[0-9]+}", s2tools.GeoJSONToCellHandler).Methods("POST")
	http.Handle("/", r)
	log.Println(http.ListenAndServe(fmt.Sprintf(":%d", *httpPort), nil))
}
