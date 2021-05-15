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
		Receive(Context)
		StopCallback() func()
	}

	Interceptor func(Message)
)

type System interface {
	Spawn(actor Actor, capacity int) ID
	Send(id ID, message Message) error
	CurrentIDs() []ID
	Stop(id ID)
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

	currentIDs chan []ID
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

	currentIDsPool = sync.Pool{
		New: func() interface{} {
			return make(chan []ID)
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

func WithMessageBufferCapacity(capacity int) SystemOption {
	return func(sys *system) {
		sys.sendMessageLane = make(chan *sendMessage, capacity)
	}
}

func WithInterceptor(interceptor Interceptor) SystemOption {
	return func(sys *system) {
		sys.interceptor = interceptor
	}
}

type system struct {
	nextID          ID
	addActorLane    chan *addActor
	removeActorLane chan *removeActor
	sendMessageLane chan *sendMessage
	currentIDsLane  chan currentIDs
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
			actor.Receive(context{id, message})
		}
		if scb := actor.StopCallback(); scb != nil {
			scb()
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
	m := removeActorPool.Get().(*removeActor)
	m.id = id
	sys.removeActorLane <- m
	<-m.done
	removeActorPool.Put(m)
}

func (sys *system) CurrentIDs() []ID {
	m := currentIDsPool.Get().(chan []ID)
	sys.currentIDsLane <- m
	ids := <-m
	currentIDsPool.Put(m)
	return ids
}

func NewSystem(options ...SystemOption) System {
	sys := &system{
		nextID:          1,
		addActorLane:    make(chan *addActor),
		removeActorLane: make(chan *removeActor),
		currentIDsLane:  make(chan currentIDs),
	}
	for _, option := range options {
		option(sys)
	}
	if sys.sendMessageLane == nil {
		sys.sendMessageLane = make(chan *sendMessage, defaultMessageLaneCapacity)
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
			case cids := <-sys.currentIDsLane:
				ids := make([]ID, 0, len(mailboxes))
				for id := range mailboxes {
					ids = append(ids, id)
				}
				cids <- ids
			}
		}
	}()
	return sys
}
