package s2tools

import (
	"github.com/golang/geo/s2"
)

// CellUnionUnion Union 2 CellUnion normalized
func CellUnionUnion(one, two s2.CellUnion) s2.CellUnion {
	one = append(one, two...)
	one.Normalize()
	return one
}

// CellUnionMissing returns cell from `from` not present in `to`
func CellUnionMissing(from, to s2.CellUnion) s2.CellUnion {
	m := make(map[s2.CellID]int)

	for _, c := range to {
		m[c]++
	}

	var ret []s2.CellID
	for _, c := range from {
		if m[c] > 0 {
			m[c]--
			continue
		}
		ret = append(ret, c)
	}

	return ret
}
