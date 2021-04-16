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

	ActorConstructor func() Actor

	Interceptor func(Message)
)

type System interface {
	Spawn(actorConstructor ActorConstructor, capacity int) ID
	Send(id ID, message Message) error
	Stop(id ID, callback func())
}

type (
	addActor struct {
		mailbox mailbox
		id      chan ID
	}

	removeActor struct {
		id   ID
		done chan struct{}
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
			return &addActor{id: make(chan ID)}
		},
	}

	removeActorPool = sync.Pool{
		New: func() interface{} {
			return &removeActor{done: make(chan struct{})}
		},
	}

	sendMessagePool = sync.Pool{
		New: func() interface{} {
			return &sendMessage{error: make(chan error)}
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
	removeActorLane chan *removeActor
	sendMessageLane chan *sendMessage
	interceptor     Interceptor
}

func (sys *system) Spawn(actorConstructor ActorConstructor, capacity int) ID {
	aa := addActorPool.Get().(*addActor)
	mailbox := newMailbox(capacity)
	aa.mailbox = mailbox
	sys.addActorLane <- aa
	id := <-aa.id
	addActorPool.Put(aa)
	go func() {
		actor := actorConstructor()
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

func (sys *system) Stop(id ID, callback func()) {
	m := removeActorPool.Get().(*removeActor)
	m.id = id
	sys.removeActorLane <- m
	<-m.done
	removeActorPool.Put(m)
	if callback != nil {
		callback()
	}
}

func NewSystem(options ...SystemOption) System {
	sys := &system{
		nextID:          1,
		addActorLane:    make(chan *addActor),
		removeActorLane: make(chan *removeActor),
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
			case ra := <-sys.removeActorLane:
				if mb, ok := mailboxes[ra.id]; ok {
					delete(mailboxes, ra.id)
					close(mb)
				}
				ra.done <- struct{}{}
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
