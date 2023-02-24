package tractor

import (
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMessageBufferCapacity is the default size of the
// buffer ingesting all user messages. It's a wild guess value
// not intended to fit for any specific use case.
const DefaultMessageBufferCapacity = 1000

// MaxActorsPerSystem is the max number of actors a system can keep.
const MaxActorsPerSystem = int64(100_000)

// InvalidID represents the invalid actor ID.
const InvalidID = ID(0)

type (
	// ID is a unique identifier associated with
	// each actor ever spawned onto the system.
	ID int64

	// Message is an opaque type representing any message
	// being sent to/received by actors.
	Message any

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
		StopCallback()
	}

	// Interceptor is a hijacking function to inspect
	// all the incomming messages and ids of their senders.
	// It's useful if the user needs cross-actor message
	// processing logic.
	Interceptor func(ID, Message)

	// SystemOption represents optional settings,
	// passed to the system constructor, which alters
	// default behavior.
	NewSystemOption func(*system)
)

// System is an isolated runtime comprising actors and interceptor(s).
type System interface {
	// Spawn starts running the given actor with the mailbox
	// capacity in the system.
	Spawn(actor Actor, capacity int, options ...SpawnOption) ID

	// Send sends the message to an actor with the given ID.
	// If the actor is not found, it returns `ErrActorNotFound` error.
	Send(id ID, message Message) error

	// CurrentActors returns IDs of the actors currently running
	// in the system.
	CurrentActors() []ID

	// MailboxSize return the number of currently pending messages
	// in the actor's mailbox (i.e. in its internal buffer). If an
	// actor with the given ID is not found, it returns a negative value.
	MailboxSize(id ID) int

	// Stop stops an actor with the given ID.
	Stop(id ID)
}

// WithMessageBufferCapacity sets the capacity of the
// system's buffer that ingests all user messages.
func WithMessageBufferCapacity(capacity int) NewSystemOption {
	return func(sys *system) {
		sys.sendMessageLane = make(chan *sendMessage, capacity)
	}
}

// WithInterceptor sets a function that intercepts all
// the incoming user messages and should be used for
// non-trivial cross-actor message processing.
func WithInterceptor(interceptor Interceptor) NewSystemOption {
	return func(sys *system) {
		sys.interceptor = interceptor
	}
}

// NewSystem creates a new system ready to spawn actors onto.
func NewSystem(options ...NewSystemOption) System {
	nextID := atomic.AddInt64(&systemID, MaxActorsPerSystem)
	startID, endID := nextID, nextID+MaxActorsPerSystem-1
	sys := &system{
		startID:           startID,
		endID:             endID,
		nextID:            ID(nextID),
		addActorLane:      make(chan *addActor),
		removeActorLane:   make(chan *removeActor),
		currentActorsLane: make(chan *currentActors),
		mailboxSizeLane:   make(chan *mailboxSize),
	}
	for _, option := range options {
		option(sys)
	}
	if sys.sendMessageLane == nil {
		sys.sendMessageLane = make(chan *sendMessage, DefaultMessageBufferCapacity)
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
				if mailbox, ok := mailboxes[ra.id]; ok {
					close(mailbox)
					delete(mailboxes, ra.id)
				}
				ra.done <- struct{}{}
			case sm := <-sys.sendMessageLane:
				if mailbox, ok := mailboxes[sm.id]; ok {
					m := sm.message
					if sys.interceptor != nil {
						sys.interceptor(sm.id, m)
					}
					sm.error <- mailbox.Put(m)
				} else {
					sm.error <- ErrActorNotFound
				}
			case cids := <-sys.currentActorsLane:
				ids := make([]ID, 0, len(mailboxes))
				for id := range mailboxes {
					ids = append(ids, id)
				}
				*cids <- ids
			case mbs := <-sys.mailboxSizeLane:
				var size = -1
				if mailbox, ok := mailboxes[mbs.id]; ok {
					size = len(mailbox)
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

	currentActors chan []ID

	mailboxSize struct {
		id   ID
		size chan int
	}
)

var (
	systemID = 1 - MaxActorsPerSystem

	addActorPool = sync.Pool{
		New: func() any {
			return &addActor{id: make(chan ID)}
		},
	}

	removeActorPool = sync.Pool{
		New: func() any {
			return &removeActor{done: make(chan struct{})}
		},
	}

	sendMessagePool = sync.Pool{
		New: func() any {
			return &sendMessage{error: make(chan error)}
		},
	}

	currentActorsPool = sync.Pool{
		New: func() any {
			c := currentActors(make(chan []ID))
			return &c
		},
	}

	mailboxSizePool = sync.Pool{
		New: func() any {
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

type (
	spawnOptions struct {
		heartbeatInterval time.Duration
	}

	// SpawnOption represents an option when spawning an actor onto the system.
	SpawnOption func(*spawnOptions)
)

// WithHeartbeatInterval adds periodic sending heartbeat
// messages to an actor to be created with the given time interval.
func WithHeartbeatInterval(interval time.Duration) SpawnOption {
	return func(so *spawnOptions) {
		so.heartbeatInterval = interval
	}
}

type system struct {
	startID           int64
	endID             int64
	nextID            ID
	addActorLane      chan *addActor
	removeActorLane   chan *removeActor
	sendMessageLane   chan *sendMessage
	currentActorsLane chan *currentActors
	mailboxSizeLane   chan *mailboxSize
	interceptor       Interceptor
}

func (sys *system) Spawn(actor Actor, capacity int, options ...SpawnOption) ID {
	aa := addActorPool.Get().(*addActor)
	mailbox := newMailbox(capacity)
	aa.mailbox = mailbox
	sys.addActorLane <- aa
	id := <-aa.id
	aa.mailbox = nil
	addActorPool.Put(aa)
	opts := &spawnOptions{}
	for _, f := range options {
		f(opts)
	}
	var stopHeartbeat chan struct{}
	if d := opts.heartbeatInterval; d != 0 {
		stopHeartbeat = make(chan struct{})
		go func() {
			timer := time.NewTimer(d)
			for {
				select {
				case <-stopHeartbeat:
					if !timer.Stop() {
						<-timer.C
					}
					return
				case <-timer.C:
					sys.Send(id, Heartbeat{})
					timer.Reset(d)
				}
			}
		}()
	}
	go func() {
		for message := range mailbox.C() {
			actor.Receive(context{id, message})
		}
		if stopHeartbeat != nil {
			close(stopHeartbeat)
		}
		actor.StopCallback()
	}()
	return id
}

func (sys *system) Send(id ID, message Message) error {
	if isIDOutOfRange(int64(id), sys.startID, sys.endID) {
		panic("incorrect id for current system")
	}
	m := sendMessagePool.Get().(*sendMessage)
	m.id = id
	m.message = message
	sys.sendMessageLane <- m
	err := <-m.error
	m.message = nil
	sendMessagePool.Put(m)
	return err
}

func (sys *system) Stop(id ID) {
	if isIDOutOfRange(int64(id), sys.startID, sys.endID) {
		panic("incorrect id for current system")
	}
	m := removeActorPool.Get().(*removeActor)
	m.id = id
	sys.removeActorLane <- m
	<-m.done
	removeActorPool.Put(m)
}

func (sys *system) CurrentActors() []ID {
	m := currentActorsPool.Get().(*currentActors)
	sys.currentActorsLane <- m
	as := <-(*m)
	currentActorsPool.Put(m)
	return as
}

func (sys *system) MailboxSize(id ID) int {
	if isIDOutOfRange(int64(id), sys.startID, sys.endID) {
		panic("incorrect id for current system")
	}
	m := mailboxSizePool.Get().(*mailboxSize)
	m.id = id
	sys.mailboxSizeLane <- m
	size := <-m.size
	mailboxSizePool.Put(m)
	return size
}

func isIDOutOfRange(currentID, startID, endID int64) bool {
	if currentID < startID || currentID > endID {
		return true
	}
	return false
}
