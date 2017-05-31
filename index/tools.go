package index

import (
	"encoding/binary"
	"math"
)

const earthCircumferenceMeter = 40075017

type GeoID []byte

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func s2RadialAreaMeters(radius float64) float64 {
	r := (radius / earthCircumferenceMeter) * math.Pi * 2
	return (math.Pi * r * r)
}
