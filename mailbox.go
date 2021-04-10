package tractor

type mailbox chan interface{}

func newMailbox(capacity int) mailbox {
	return make(chan interface{}, capacity)
}

func (mb mailbox) Put(message interface{}) (err error) {
	select {
	case mb <- message:
	default:
		err = ErrFullActorMailbox
	}
	return
}

func (mb mailbox) Get() interface{} {
	return <-mb
}
