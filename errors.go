package tractor

const (
	// ErrActorNotFound indicates that actor is not
	// found for some reason.
	ErrActorNotFound = _Error("actor not found")

	// ErrFullActorMailbox indicates that the actor's
	// mailbox is full.
	ErrFullActorMailbox = _Error("actor mailbox is full")

	// ErrWrongSystem indicates that the given ID
	// does not belong to the current system.
	ErrWrongSystem = _Error("wrong system")
)

type _Error string

func (e _Error) Error() string {
	return string(e)
}
