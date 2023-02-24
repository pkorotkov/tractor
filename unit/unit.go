package unit

import (
	"sync/atomic"

	"github.com/pkorotkov/tractor"
)

// ExtendedActor is an extended version of Actor
// interface to make easier work with units.
type ExtendedActor interface {
	tractor.Actor
	SetSelf(*Unit)
}

// Unit is a lightweight wrapper for raw actors
// keeping information about the actor's system
// and its running status. This prevents accidental
// uses of a wrong system for an actor.
type Unit struct {
	system  tractor.System
	actorID tractor.ID
	stopped *atomic.Bool
}

// New creates a new unit (spawning an actor within the system given) in similar ways we create an actor.
func New(system tractor.System, actor ExtendedActor, capacity int, options ...tractor.SpawnOption) *Unit {
	var stopped atomic.Bool
	unit := &Unit{system: system, stopped: &stopped}
	actor.SetSelf(unit)
	unit.actorID = system.Spawn(actor, capacity, options...)
	return unit
}

// Send sends a message to the unit (actor).
func (unit *Unit) Send(message tractor.Message) error {
	return unit.system.Send(unit.actorID, message)
}

// Stop stops the unit (actor).
func (unit *Unit) Stop() {
	unit.system.Stop(unit.actorID)
	unit.stopped.Store(true)
}

// Stopped reports whether the unit (actor) has been stopped.
func (unit *Unit) Stopped() bool {
	return unit.stopped.Load()
}

// MailboxSize returns mailbox size of the underlying actor.
func (unit *Unit) MailboxSize() int {
	return unit.system.MailboxSize(unit.actorID)
}
