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

func TestCurrentActors(t *testing.T) {
	t.Run("current-actors", func(t *testing.T) {
		c := qt.New(t)
		s := NewSystem()
		s.Spawn(&testActor{}, 1)
		s.Spawn(&testActor{}, 1)
		s.Spawn(&testActor{}, 1)
		cas := s.CurrentActors()
		c.Assert(cas, qt.DeepEquals, []ID{1, 2, 3})
	})
}

type testActor struct{}

func (ta *testActor) Receive(_ Context) {}

func (ta *testActor) StopCallback() func() {
	return nil
}
