package tractor

import (
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestSystem(t *testing.T) {
	t.Run("concurrent-system-creation", func(t *testing.T) {
		c := qt.New(t)
		ids := make(chan ID)
		gotIDs := make([]ID, 0, 3)
		expectedIDs := []ID{
			1,
			1 + ID(1*MaxActorsPerSystem),
			1 + ID(2*MaxActorsPerSystem),
		}
		for i := 0; i < 3; i++ {
			go func() {
				s := NewSystem()
				ids <- s.(*system).nextID
			}()
		}
		for i := 0; i < 3; i++ {
			gotIDs = append(gotIDs, <-ids)
		}
		sort.Slice(gotIDs, func(i, j int) bool { return gotIDs[i] < gotIDs[j] })
		c.Assert(gotIDs, qt.DeepEquals, expectedIDs)
	})
}
