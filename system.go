package tractor

import (
	"sync"
)

const defaultMessageLaneCapacity = 1000

type (
	// ID is a unique identifier associated with
	// each actor at the system.
	ID uint64

	// Message is an opaque type representing any message
	// being sent to/received by actors.
	Message interface{}

	// Context is a handy wrapper for a received message
	// along with an ID of the actor processing the message.
	Context interface {
		// Self returns the ID of the actor that processes
		// the current message.
		Self() ID

		// Message is an incomming message to be processed.
		Message() Message
	}

	// Actor is a contract to implement for objects
	// which can be actors.
	Actor interface {
		// Receive keeps the business logic of how
		// an actor handles messages. This method is
		// supposed to be non-blocking and change
		// the actor's state synchronously.
		Receive(Context)

		// StopCallback is a funtion called right after
		// stopping processing messages but before the
		// final wiping the actor out. Note that the actor's
		// dedicated goroutine exists unless this function
		// finishes.
		StopCallback() func()
	}

	// Interceptor is a hijacking function to inspect
	// all incomming messages useful, if the user needs
	// cross-actor message processing logic.
	Interceptor func(Message)
)

// System is an isolated runtime comprising actors and interceptors.
type System interface {
	// Spawn starts running the given actor with the mailbox capacity
	// at the system.
	Spawn(actor Actor, capacity int) ID

	// Send sends the message to an actor with the given ID.
	// If the actor is not found, it returns `ErrActorNotFound`.
	Send(id ID, message Message) error

	// CurrentIDs returns the IDs of the actors currently
	// running at the system.
	CurrentIDs() []ID

	// MailboxSize return the number of currently pending messages
	// in the actor's mailbox (i.e. in its internal buffer).
	MailboxSize(id ID) int

	// Stop stops an actor with the given ID.
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

	mailboxSize struct {
		id   ID
		size chan int
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

	currentIDsPool = sync.Pool{
		New: func() interface{} {
			return make(chan []ID)
		},
	}

	mailboxSizePool = sync.Pool{
		New: func() interface{} {
			return &mailboxSize{size: make(chan int)}
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

// SystemOption represents an optional setting,
// passed to the system constructor, which alters
// default behavior.
type SystemOption func(*system)

// WithMessageBufferCapacity sets the capacity of system's
// buffer that ingests all the incomming messages including
// both user and service ones.
func WithMessageBufferCapacity(capacity int) SystemOption {
	return func(sys *system) {
		sys.sendMessageLane = make(chan *sendMessage, capacity)
	}
}

// WithInterceptor sets a function that intercepts all
// the incomming user messages and should be used for
// non-trivial cross-actor message processing.
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
	mailboxSizeLane chan *mailboxSize
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

func (sys *system) MailboxSize(id ID) int {
	m := mailboxSizePool.Get().(*mailboxSize)
	m.id = id
	sys.mailboxSizeLane <- m
	size := <-m.size
	mailboxSizePool.Put(m)
	return size
}

// NewSystem create a new system ready to spawn actors onto.
func NewSystem(options ...SystemOption) System {
	sys := &system{
		nextID:          1,
		addActorLane:    make(chan *addActor),
		removeActorLane: make(chan *removeActor),
		currentIDsLane:  make(chan currentIDs),
		mailboxSizeLane: make(chan *mailboxSize),
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
			case mbs := <-sys.mailboxSizeLane:
				var size int
				if mb, ok := mailboxes[mbs.id]; ok {
					size = len(mb)
				}
				mbs.size <- size
			}
		}
	}()
	return sys
}
