package tractor

import (
	"sync"
)

const defaultMessageLaneCapacity = 1000

type (
	Message interface{}

	Actor interface {
		Receive(Message)
	}

	ID uint64

	Interceptor func(Message)
)

type systemOption struct {
	interceptor Interceptor
}

type SystemOption func(systemOption)

func WithInterceptor(interceptor Interceptor) SystemOption {
	return func(mbo systemOption) {
		mbo.interceptor = interceptor
	}
}

type System interface {
	Spawn(Actor, int) ID
	Send(ID, Message) error
}

type (
	addActor struct {
		mailbox mailbox
		id      chan ID
	}
	sendMessage struct {
		id      ID
		message Message
		error   chan error
	}
)

var (
	addActorPool = sync.Pool{
		New: func() interface{} {
			return &addActor{
				id: make(chan ID),
			}
		},
	}
	sendMessagePool = sync.Pool{
		New: func() interface{} {
			return &sendMessage{
				error: make(chan error),
			}
		},
	}
)

type system struct {
	nextID          ID
	addActorLane    chan *addActor
	removeActorLane chan ID
	sendMessageLane chan *sendMessage
}

func (sys *system) Spawn(actor Actor, capacity int) ID {
	aa := addActorPool.Get().(*addActor)
	mailbox := newMailbox(capacity)
	go func() {
		for {
			m := mailbox.Get()
			actor.Receive(m)
		}
	}()
	aa.mailbox = mailbox
	sys.addActorLane <- aa
	id := <-aa.id
	addActorPool.Put(aa)
	return id
}

func (sys *system) Send(id ID, message Message) error {
	m := sendMessagePool.Get().(*sendMessage)
	m.id = id
	m.message = message
	sys.sendMessageLane <- m
	err := <-m.error
	sendMessagePool.Put(m)
	return err
}

func NewSystem(options ...SystemOption) System {
	sys := &system{
		nextID:          1,
		addActorLane:    make(chan *addActor),
		removeActorLane: make(chan ID),
		sendMessageLane: make(chan *sendMessage, defaultMessageLaneCapacity),
	}
	go func() {
		mailboxes := make(map[ID]mailbox)
		for {
			select {
			case aa := <-sys.addActorLane:
				id := sys.nextID
				mailboxes[id] = aa.mailbox
				sys.nextID += 1
				aa.id <- id
			case id := <-sys.removeActorLane:
				if a, ok := mailboxes[id]; ok {
					// TODO
					_ = a
					delete(mailboxes, id)
				}
			case m := <-sys.sendMessageLane:
				if mb, ok := mailboxes[m.id]; ok {
					m.error <- mb.Put(m.message)
				} else {
					m.error <- ErrActorNotFound
				}
			}
		}
	}()
	return sys
}
