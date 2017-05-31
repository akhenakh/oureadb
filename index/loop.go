package index

import "github.com/golang/geo/s2"

// LoopFence Making s2.Loop implements methods needed for coverage
type LoopFence struct {
	*s2.Loop
}

// LoopFenceFromPoints creates a LoopFence from a list of s2.Point
func LoopFenceFromPoints(points []s2.Point) *LoopFence {
	loop := s2.LoopFromPoints(points)
	return &LoopFence{loop}
}

// LoopFenceFromCoordinates creates a LoopFence from a list of lng lat
func LoopFenceFromCoordinates(c []float64) *LoopFence {
	points := make([]s2.Point, len(c)/2)

	for i := 0; i < len(c); i += 2 {
		points[i/2] = s2.PointFromLatLng(s2.LatLngFromDegrees(c[i+1], c[i]))
	}
	loop := s2.LoopFromPoints(points)
	return &LoopFence{loop}
}

// CapBound returns the cap that contains this loop
func (l *LoopFence) CapBound() s2.Cap {
	return l.Loop.CapBound()
}

// ContainsCell checks whether the cell is completely enclosed by this loop.
// Does not count for loop interior and uses raycasting.
func (l *LoopFence) ContainsCell(c s2.Cell) bool {
	for i := 0; i < 4; i++ {
		v := c.Vertex(i)
		if !l.ContainsPoint(v) {
			return false
		}
	}
	return true
}

// IntersectsCell checks if any edge of the cell intersects the loop or if the cell is contained.
func (l *LoopFence) IntersectsCell(c s2.Cell) bool {
	for i := 0; i < 4; i++ {
		crosser := s2.NewChainEdgeCrosser(c.Vertex(i), c.Vertex((i+1)%4), l.Vertex(0))
		for _, v := range l.Vertices()[1:] {
			if crosser.EdgeOrVertexChainCrossing(v) {
				return true
			}
		}
		if crosser.EdgeOrVertexChainCrossing(l.Vertex(0)) { //close the loop
			return true
		}
	}
	return l.ContainsCell(c)
}
