[![Go Report Card](https://goreportcard.com/badge/github.com/akhenakh/oureadb)](https://goreportcard.com/report/github.com/akhenakh/oureadb)  [![GoDoc](https://godoc.org/github.com/akhenakh/oureadb/index?status.svg)](https://godoc.org/github.com/akhenakh/oureadb/index)

## OureaDB

A general purpose geo data storing and indexing tool.

GeoJSON can be geo indexed and stored in protobuf (see `TestGeoJSONFeatureToGeoData()`)

- S2FlatIdx a points, lines & polygons indexer, flat cover using s2

- S2FlatTimeIdx a geo reverse timed points, lines & polygons indexer, flat cover using s2

- S2PointIdx a point only generic indexer using s2

