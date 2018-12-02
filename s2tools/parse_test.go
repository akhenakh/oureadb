package s2tools

import (
	"testing"

	"github.com/golang/geo/s2"
	"github.com/stretchr/testify/require"
)

func TestParseCellID(t *testing.T) {
	co := s2.CellIDFromToken("47e66f")
	t.Log(co)
	c := ParseCellID("2/0333030313")
	require.NotNil(t, c)
	require.Equal(t, co, co)

	c = ParseCellID("47e66f")
	require.NotNil(t, c)
	require.Equal(t, co, co)

	c = ParseCellID("47e66f433219aa83")
	require.NotNil(t, c)
	require.Equal(t, co, co)

	c = ParseCellID("x")
	require.Nil(t, c)

	c = ParseCellID("6/122")
	require.Nil(t, c)
}
