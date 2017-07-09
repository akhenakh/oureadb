package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/akhenakh/oureadb/s2tools"
)

var (
	httpPort = flag.Int("httpPort", 8000, "http debug port to listen on")
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	flag.Parse()

	http.HandleFunc("/api/s2cells", s2tools.S2CellQueryHandler)

	log.Println(http.ListenAndServe(fmt.Sprintf(":%d", *httpPort), nil))
}
