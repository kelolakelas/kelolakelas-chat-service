package chat

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
)

// WS realtime fan-out constants from the KEL-121 contract.
const (
	// WSEventMessageCreated is sent with the conversation ID and message when
	// another participant sends via REST.
	WSEventMessageCreated = "message.created"
	// WSEventConversationRead is sent with the conversation ID, reader user
	// ID, and read marker when a participant marks a conversation read.
	WSEventConversationRead = "conversation.read"
	// WSMaxConnsPerUser caps simultaneous WS connections per user against abuse.
	WSMaxConnsPerUser = 5
	// WSConnSendBuffer bounds queued events per connection; a consumer that
	// falls behind is disconnected instead of blocking the hub.
	WSConnSendBuffer = 16
)

// WSEvent is the server-to-client WS payload. The socket is server-to-client
// only (besides ping/pong); messages keep flowing through REST.
type WSEvent struct {
	Type           string     `json:"type"`
	ConversationID uuid.UUID  `json:"conversation_id"`
	Message        *Message   `json:"message,omitempty"`
	UserID         *uuid.UUID `json:"user_id,omitempty"`
	ReadAt         *time.Time `json:"read_at,omitempty"`
}

// WSConnection is one registered realtime subscription. Actor carries the
// chat:manage/report:read rights frozen at connect time; Out receives events
// while registered. The hub closes Out when the connection is evicted
// (slow consumer) or shut down; the delivery layer translates that into a
// normal-close frame.
type WSConnection struct {
	actor Actor
	out   chan []byte
}

// Actor returns the subscriber with connect-time rights.
func (c *WSConnection) Actor() Actor { return c.actor }

// Out receives queued events; it is closed on eviction or hub shutdown.
func (c *WSConnection) Out() <-chan []byte { return c.out }

// WSHub is the in-memory fan-out for a single instance (multi-instance
// fan-out is explicitly out of scope). All methods are safe for concurrent
// use. Broadcasts never block: a connection whose buffer is full is evicted
// so one slow client cannot stall delivery to the rest.
type WSHub struct {
	mu      sync.Mutex
	conns   map[*WSConnection]struct{}
	perUser map[uuid.UUID]int
}

// NewWSHub returns an empty hub.
func NewWSHub() *WSHub {
	return &WSHub{conns: make(map[*WSConnection]struct{}), perUser: make(map[uuid.UUID]int)}
}

// Register subscribes one connection. It refuses when the user already holds
// WSMaxConnsPerUser connections.
func (h *WSHub) Register(a Actor) (*WSConnection, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.perUser[a.UserID] >= WSMaxConnsPerUser {
		return nil, false
	}
	c := &WSConnection{actor: a, out: make(chan []byte, WSConnSendBuffer)}
	h.conns[c] = struct{}{}
	h.perUser[a.UserID]++
	return c, true
}

// Unregister drops a connection; it is idempotent so eviction and the
// connection pump can both call it.
func (h *WSHub) Unregister(c *WSConnection) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.conns[c]; !ok {
		return
	}
	delete(h.conns, c)
	if h.perUser[c.actor.UserID] > 1 {
		h.perUser[c.actor.UserID]--
	} else {
		delete(h.perUser, c.actor.UserID)
	}
}

// ConnCount reports live connections; used by tests.
func (h *WSHub) ConnCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns)
}

// visibleFrozen mirrors Service.Visible but uses the connect-time frozen
// rights instead of live permission checks, per the contract ("hak
// dievaluasi saat terhubung"). Any drift between the two is a leak-class
// bug; the isolation tests pin both to the same matrix.
func visibleFrozen(a Actor, c Conversation) bool {
	if a.UserID == uuid.Nil {
		return false
	}
	if c.Kind != "staff" && c.Kind != "schedule_request" && c.Kind != "report" {
		return false
	}
	if c.Kind != "staff" && a.IsParent {
		return c.ParentUserID != nil && *c.ParentUserID == a.UserID
	}
	if a.IsParent || a.TenantID == uuid.Nil || a.TenantID != c.TenantID {
		return false
	}
	if c.Kind == "staff" && c.MemberUserID != nil && *c.MemberUserID == a.UserID {
		return true
	}
	if c.Kind == "report" {
		return a.CanReport
	}
	return a.CanManage
}

// broadcast delivers payload only to connections that can see conv. Slow
// consumers are evicted (channel closed) without blocking the rest.
func (h *WSHub) broadcast(conv Conversation, payload []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.conns {
		if !visibleFrozen(c.actor, conv) {
			continue
		}
		select {
		case c.out <- payload:
		default:
			delete(h.conns, c)
			if h.perUser[c.actor.UserID] > 1 {
				h.perUser[c.actor.UserID]--
			} else {
				delete(h.perUser, c.actor.UserID)
			}
			close(c.out)
		}
	}
}

// BroadcastMessage fans out a message.created event; failures to marshal
// cannot occur for store-shaped messages, and a broadcast never fails the
// REST send it follows.
func (h *WSHub) BroadcastMessage(conv Conversation, m Message) {
	payload, err := json.Marshal(WSEvent{Type: WSEventMessageCreated, ConversationID: conv.ID, Message: &m})
	if err != nil {
		return
	}
	h.broadcast(conv, payload)
}

// BroadcastRead fans out a conversation.read event.
func (h *WSHub) BroadcastRead(conv Conversation, user uuid.UUID, at time.Time) {
	payload, err := json.Marshal(WSEvent{Type: WSEventConversationRead, ConversationID: conv.ID, UserID: &user, ReadAt: &at})
	if err != nil {
		return
	}
	h.broadcast(conv, payload)
}

// Close drops every connection with a closed channel so pumps exit with a
// normal close code on shutdown. The hub remains usable afterwards.
func (h *WSHub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.conns {
		close(c.out)
	}
	h.conns = make(map[*WSConnection]struct{})
	h.perUser = make(map[uuid.UUID]int)
}
