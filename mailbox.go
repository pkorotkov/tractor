package tractor

type mailbox chan Message

func newMailbox(capacity int) mailbox {
	return make(chan Message, capacity)
}

func (mb mailbox) Put(message Message) (err error) {
	select {
	case mb <- message:
	default:
		err = ErrFullActorMailbox
	}
	return
}

func (mb mailbox) C() <-chan Message {
	return mb
}
