package tractor

type _Error string

func (e _Error) Error() string {
	return string(e)
}

const (
	ErrActorNotFound    = _Error("actor not found")
	ErrFullActorMailbox = _Error("actor mailbox is full")
)
