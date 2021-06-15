package tractor

const (
	// ErrActorNotFound indicates that actor is not
	// found for some reason.
	ErrActorNotFound = _Error("actor not found")

	// ErrFullActorMailbox indicates that the actor's
	// mailbox is full.
	ErrFullActorMailbox = _Error("actor mailbox is full")
)

type _Error string

func (e _Error) Error() string {
	return string(e)
}
