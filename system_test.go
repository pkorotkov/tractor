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
		const n = 4
		gotIDs := make([]ID, 0, n)
		expectedIDs := []ID{
			1,
			1 + ID(1*MaxActorsPerSystem),
			1 + ID(2*MaxActorsPerSystem),
			1 + ID(3*MaxActorsPerSystem),
		}
		for i := 0; i < n; i++ {
			go func() {
				s := NewSystem()
				ids <- s.(*system).nextID
			}()
		}
		for i := 0; i < n; i++ {
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
		sort.Slice(cas, func(i, j int) bool { return cas[i] < cas[j] })
		c.Assert(cas, qt.DeepEquals, []ID{1, 2, 3})
	})
}

type testActor struct{}

func (ta *testActor) Receive(_ Context) {}

func (ta *testActor) StopCallback() {}

func TestActorNotFound(t *testing.T) {
	t.Run("actor-not-found", func(t *testing.T) {
		c := qt.New(t)
		s := NewSystem()
		_ = s.Spawn(&testActor{}, 1)
		err := s.Send(ID(MaxActorsPerSystem), nil)
		c.Assert(err, qt.ErrorIs, ErrActorNotFound)
	})
}
