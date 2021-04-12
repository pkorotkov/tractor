package tractor

import (
	"sync"
)

const defaultMessageLaneCapacity = 1000

type (
	ID uint64

	Message interface{}

	Context interface {
		Self() ID
		Message() Message
	}

	Actor interface {
		Receive(context Context)
	}

	Interceptor func(Message)
)

type System interface {
	Spawn(actor Actor, capacity int) ID
	Send(id ID, message Message) error
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

type context struct {
	id      ID
	message Message
}

func (ctx context) Self() ID {
	return ctx.id
}

func (ctx context) Message() Message {
	return ctx.message
}

type SystemOption func(*system)

func WithInterceptor(interceptor Interceptor) SystemOption {
	return func(s *system) {
		s.interceptor = interceptor
	}
}

type system struct {
	nextID          ID
	addActorLane    chan *addActor
	removeActorLane chan ID
	sendMessageLane chan *sendMessage
	interceptor     Interceptor
}

func (sys *system) Spawn(actor Actor, capacity int) ID {
	aa := addActorPool.Get().(*addActor)
	mailbox := newMailbox(capacity)
	aa.mailbox = mailbox
	sys.addActorLane <- aa
	id := <-aa.id
	addActorPool.Put(aa)
	go func() {
		for message := range mailbox.C() {
			ctx := context{id, message}
			actor.Receive(ctx)
		}
	}()
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

func (sys *system) Stop(id ID) {
	sys.removeActorLane <- id
}

func NewSystem(options ...SystemOption) System {
	sys := &system{
		nextID:          1,
		addActorLane:    make(chan *addActor),
		removeActorLane: make(chan ID),
		sendMessageLane: make(chan *sendMessage, defaultMessageLaneCapacity),
	}
	for _, option := range options {
		option(sys)
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
				if mb, ok := mailboxes[id]; ok {
					delete(mailboxes, id)
					close(mb)
				}
			case sm := <-sys.sendMessageLane:
				if mb, ok := mailboxes[sm.id]; ok {
					if sys.interceptor != nil {
						sys.interceptor(sm.message)
					}
					sm.error <- mb.Put(sm.message)
				} else {
					sm.error <- ErrActorNotFound
				}
			}
		}
	}()
	return sys
}
