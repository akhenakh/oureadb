package s2tools

import (
	"testing"

	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
)

func TestCellUnionMissing(t *testing.T) {
	cu := s2.CellUnion{6093496101150654464, 6093496108309282816, 6093496138374053888, 6093496159848890368, 6093490069585264640}
	pcu := s2.CellUnion{6093496101150654464, 6093496108309282816, 6093496138374053888, 6093496159848890368, 6093489644383502336, 6093490026635591680}

	res := CellUnionMissing(cu, pcu)
	require.Len(t, res, 1)
	require.EqualValues(t, 6093490069585264640, uint64(res[0]))

	res = CellUnionMissing(pcu, cu)
	require.Len(t, res, 2)

	require.EqualValues(t, 6093489644383502336, uint64(res[0]))
	require.EqualValues(t, 6093490026635591680, uint64(res[1]))
}
