package tractor

import (
	"sync"
	"sync/atomic"
)

// DefaultMessageBufferCapacity is the default size of the
// buffer ingesting all user messages. It's a wild guess value
// not intended to fit for any specific use case.
const DefaultMessageBufferCapacity = 1000

// MaxActors is the max number of actors a system can keep.
const MaxActorsPerSystem = int64(100_000)

type (
	// ID is a unique identifier associated with
	// each actor ever created at the system.
	ID int64

	// Message is an opaque type representing any message
	// being sent to/received by actors.
	Message interface{}

	// Context is a handy wrapper for the received message.
	Context interface {
		// SelfID returns the ID of the actor that
		// processes the current message.
		SelfID() ID

		// Message is an incomming message to be
		// processed on the actor.
		Message() Message
	}

	// Actor is a contract to be implemented by
	// objects to become actors.
	Actor interface {
		// Receive keeps business logic of how
		// an actor handles messages. This method is
		// supposed to be non-blocking and change
		// the actor's state synchronously.
		Receive(Context)

		// StopCallback is a funtion called right after
		// stopping receiving messages but before
		// wiping the actor out. Note that the actor's
		// dedicated goroutine exists unless this function
		// finishes.
		StopCallback() func()
	}

	// Interceptor is a hijacking function to inspect
	// all the incomming messages. It's useful if the
	// user needs cross-actor message processing logic.
	Interceptor func(ID, Message)

	// SystemOption represents optional settings,
	// passed to the system constructor, which alters
	// default behavior.
	SystemOption func(*system)

	// SpawnOption represents optional spawn settings.
	SpawnOption func(*spawnOptions)

	// ActorProfile combines actor's ID and name.
	ActorProfile struct {
		id   ID
		name string
	}
)

// System is an isolated runtime comprising actors and interceptor(s).
type System interface {
	// Spawn starts running the given actor with the mailbox
	// capacity at the system.
	Spawn(actor Actor, capacity int, options ...SpawnOption) ID

	// Send sends the message to an actor with the given ID.
	// If the actor is not found, it returns `ErrActorNotFound` error.
	Send(id ID, message Message) error

	// CurrentActors returns pairs of ID and name of the actors currently
	// running at the system.
	CurrentActors() []ActorProfile

	// MailboxSize return the number of currently pending messages
	// in the actor's mailbox (i.e. in its internal buffer). If an
	// actor with the given ID is not found, it returns a negative value.
	MailboxSize(id ID) int

	// Stop stops an actor with the given ID.
	Stop(id ID)
}

// WithMessageBufferCapacity sets the capacity of the
// system's buffer that ingests all user messages.
func WithMessageBufferCapacity(capacity int) SystemOption {
	return func(sys *system) {
		sys.sendMessageLane = make(chan *sendMessage, capacity)
	}
}

// WithInterceptor sets a function that intercepts all
// the incoming user messages and should be used for
// non-trivial cross-actor message processing.
func WithInterceptor(interceptor Interceptor) SystemOption {
	return func(sys *system) {
		sys.interceptor = interceptor
	}
}

// WithName sets actor's human-readable name.
func WithName(name string) SpawnOption {
	return func(s *spawnOptions) {
		s.actorName = name
	}
}

// NewSystem create a new system ready to spawn actors onto.
func NewSystem(options ...SystemOption) System {
	sys := &system{
		nextID:            ID(atomic.AddInt64(&systemID, MaxActorsPerSystem)),
		addActorLane:      make(chan *addActor),
		removeActorLane:   make(chan *removeActor),
		currentActorsLane: make(chan currentActors),
		mailboxSizeLane:   make(chan *mailboxSize),
	}
	for _, option := range options {
		option(sys)
	}
	if sys.sendMessageLane == nil {
		sys.sendMessageLane = make(chan *sendMessage, DefaultMessageBufferCapacity)
	}
	go func() {
		actors := make(map[ID]actor)
		for {
			select {
			case aa := <-sys.addActorLane:
				id := sys.nextID
				actors[id] = actor{mailbox: aa.mailbox, name: aa.name}
				sys.nextID += 1
				aa.id <- id
			case ra := <-sys.removeActorLane:
				if actor, ok := actors[ra.id]; ok {
					close(actor.mailbox)
					delete(actors, ra.id)
				}
				ra.done <- struct{}{}
			case sm := <-sys.sendMessageLane:
				if actor, ok := actors[sm.id]; ok {
					if sys.interceptor != nil {
						sys.interceptor(sm.id, sm.message)
					}
					sm.error <- actor.mailbox.Put(sm.message)
				} else {
					sm.error <- ErrActorNotFound
				}
			case cas := <-sys.currentActorsLane:
				as := make([]ActorProfile, 0, len(actors))
				for id, a := range actors {
					as = append(as, ActorProfile{id: id, name: a.name})
				}
				cas <- as
			case mbs := <-sys.mailboxSizeLane:
				var size = -1
				if actor, ok := actors[mbs.id]; ok {
					size = len(actor.mailbox)
				}
				mbs.size <- size
			}
		}
	}()
	return sys
}

type (
	addActor struct {
		mailbox mailbox
		id      chan ID
		name    string
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

	currentActors chan []ActorProfile

	mailboxSize struct {
		id   ID
		size chan int
	}

	actor struct {
		mailbox mailbox
		name    string
	}
)

var (
	systemID = 1 - MaxActorsPerSystem

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

	currentActorsPool = sync.Pool{
		New: func() interface{} {
			return make(chan []ActorProfile)
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

func (ctx context) SelfID() ID {
	return ctx.id
}

func (ctx context) Message() Message {
	return ctx.message
}

type spawnOptions struct {
	actorName string
}

type system struct {
	nextID            ID
	addActorLane      chan *addActor
	removeActorLane   chan *removeActor
	sendMessageLane   chan *sendMessage
	currentActorsLane chan currentActors
	mailboxSizeLane   chan *mailboxSize
	interceptor       Interceptor
}

func (sys *system) Spawn(actor Actor, capacity int, options ...SpawnOption) ID {
	var sos spawnOptions
	for _, option := range options {
		option(&sos)
	}
	aa := addActorPool.Get().(*addActor)
	mailbox := newMailbox(capacity)
	aa.mailbox = mailbox
	aa.name = sos.actorName
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

func (sys *system) CurrentActors() []ActorProfile {
	m := currentActorsPool.Get().(chan []ActorProfile)
	sys.currentActorsLane <- m
	as := <-m
	currentActorsPool.Put(m)
	return as
}

func (sys *system) MailboxSize(id ID) int {
	m := mailboxSizePool.Get().(*mailboxSize)
	m.id = id
	sys.mailboxSizeLane <- m
	size := <-m.size
	mailboxSizePool.Put(m)
	return size
}
