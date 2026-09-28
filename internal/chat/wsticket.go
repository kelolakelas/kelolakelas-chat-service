package chat

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"sync"
	"time"
)

// WS realtime ticket parameters from the KEL-121 contract.
const (
	// WSTicketTTL caps ticket lifetime at 60 seconds.
	WSTicketTTL = 60 * time.Second
	// WSTicketBytes is the minimum ticket entropy (32 random bytes).
	WSTicketBytes = 32
)

type ticketEntry struct {
	actor     Actor
	tokenExp  time.Time
	expiresAt time.Time
}

// TicketStore keeps single-use WebSocket tickets bound to the JWT claims they
// were issued from. Only the SHA-256 hash of a ticket is kept at rest; the
// plaintext ticket is returned once at issuance and never stored. Entries are
// short-lived (WSTicketTTL), so expired entries are purged lazily on Issue
// instead of a background sweeper. A process restart drops outstanding
// tickets and clients simply mint a new one.
type TicketStore struct {
	mu    sync.Mutex
	items map[string]ticketEntry
	now   func() time.Time
}

// NewTicketStore returns an empty ticket store.
func NewTicketStore() *TicketStore {
	return &TicketStore{items: make(map[string]ticketEntry), now: time.Now}
}

// ticketHash returns the hex-encoded SHA-256 of a ticket. Tickets travel in
// URLs, so hashing also normalizes any encoding differences.
func ticketHash(ticket string) string {
	sum := sha256.Sum256([]byte(ticket))
	return hex.EncodeToString(sum[:])
}

// Issue mints a ticket with at least WSTicketBytes of crypto-random entropy,
// valid for WSTicketTTL. The plaintext is returned to the caller; only its
// hash is stored. A crypto/rand failure returns an empty ticket so the caller
// can answer 500 instead of issuing weak tickets.
func (s *TicketStore) Issue(a Actor, tokenExp time.Time) (string, time.Time) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeLocked(now)
	raw := make([]byte, WSTicketBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", time.Time{}
	}
	ticket := base64.RawURLEncoding.EncodeToString(raw)
	expiresAt := now.Add(WSTicketTTL)
	s.items[ticketHash(ticket)] = ticketEntry{actor: a, tokenExp: tokenExp, expiresAt: expiresAt}
	return ticket, expiresAt
}

// Consume atomically validates and burns a ticket. Exactly one concurrent
// caller wins: the entry is deleted under the store lock on both success and
// failure. It fails when the ticket is unknown, already used, past its TTL,
// or past the bound token expiry. The plaintext ticket is hashed for lookup
// and never retained.
func (s *TicketStore) Consume(ticket string) (Actor, time.Time, bool) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ticketHash(ticket)
	entry, ok := s.items[key]
	if !ok {
		return Actor{}, time.Time{}, false
	}
	delete(s.items, key)
	if !now.Before(entry.expiresAt) {
		return Actor{}, time.Time{}, false
	}
	if !entry.tokenExp.IsZero() && !now.Before(entry.tokenExp) {
		return Actor{}, time.Time{}, false
	}
	return entry.actor, entry.tokenExp, true
}

// purgeLocked drops expired entries. Callers must hold s.mu.
func (s *TicketStore) purgeLocked(now time.Time) {
	for key, entry := range s.items {
		if !now.Before(entry.expiresAt) || (!entry.tokenExp.IsZero() && !now.Before(entry.tokenExp)) {
			delete(s.items, key)
		}
	}
}
