package chat

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testTicketActor() Actor {
	return Actor{UserID: uuid.New(), TenantID: uuid.New(), RoleID: uuid.New(), MemberID: uuid.New()}
}

func TestTicketIssueEntropyHashAndTTL(t *testing.T) {
	s := NewTicketStore()
	before := time.Now()
	a := testTicketActor()
	tokenExp := before.Add(time.Hour)
	ticket, expiresAt := s.Issue(a, tokenExp)
	if ticket == "" {
		t.Fatal("empty ticket")
	}
	raw, err := base64.RawURLEncoding.DecodeString(ticket)
	if err != nil {
		t.Fatalf("ticket not base64url: %v", err)
	}
	if len(raw) < WSTicketBytes {
		t.Fatalf("ticket entropy %d bytes, want >= %d", len(raw), WSTicketBytes)
	}
	if ttl := expiresAt.Sub(before); ttl > WSTicketTTL+time.Second || ttl < WSTicketTTL-time.Second {
		t.Fatalf("ticket TTL %v, want ~60s", ttl)
	}
	// Only the hash is kept at rest: the stored key must be the SHA-256 hex
	// of the ticket, and the plaintext must appear nowhere in the store.
	sum := sha256.Sum256([]byte(ticket))
	wantKey := hex.EncodeToString(sum[:])
	entry, ok := s.items[wantKey]
	if !ok {
		t.Fatal("ticket hash not stored")
	}
	if entry.actor != a || !entry.tokenExp.Equal(tokenExp) {
		t.Fatalf("stored claims drifted: %+v", entry)
	}
	for key := range s.items {
		if key == ticket {
			t.Fatal("plaintext ticket stored at rest")
		}
	}
}

func TestTicketConsumeSingleUse(t *testing.T) {
	s := NewTicketStore()
	a := testTicketActor()
	tokenExp := time.Now().Add(time.Hour)
	ticket, _ := s.Issue(a, tokenExp)
	got, gotExp, ok := s.Consume(ticket)
	if !ok || got != a || !gotExp.Equal(tokenExp) {
		t.Fatalf("first consume: ok=%v exp=%v actor=%+v", ok, gotExp, got)
	}
	if _, _, ok := s.Consume(ticket); ok {
		t.Fatal("reused ticket consumed twice")
	}
	if len(s.items) != 0 {
		t.Fatalf("burned ticket left %d entries", len(s.items))
	}
}

func TestTicketUnknownRejected(t *testing.T) {
	s := NewTicketStore()
	for _, bad := range []string{"", "not-a-ticket", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"} {
		if _, _, ok := s.Consume(bad); ok {
			t.Fatalf("random ticket %q accepted", bad)
		}
	}
}

func TestTicketExpired(t *testing.T) {
	s := NewTicketStore()
	ticket, _ := s.Issue(testTicketActor(), time.Now().Add(time.Hour))
	s.now = func() time.Time { return time.Now().Add(2 * WSTicketTTL) }
	if _, _, ok := s.Consume(ticket); ok {
		t.Fatal("expired ticket accepted")
	}
}

func TestTicketBoundToTokenExpiry(t *testing.T) {
	s := NewTicketStore()
	// A ticket minted from an already-expired token burns on first use.
	ticket, _ := s.Issue(testTicketActor(), time.Now().Add(-time.Second))
	if _, _, ok := s.Consume(ticket); ok {
		t.Fatal("ticket past token exp accepted")
	}
	// A ticket outliving its token expiry between issue and use is rejected.
	s2 := NewTicketStore()
	fresh, _ := s2.Issue(testTicketActor(), time.Now().Add(50*time.Millisecond))
	time.Sleep(100 * time.Millisecond)
	if _, _, ok := s2.Consume(fresh); ok {
		t.Fatal("ticket used past token exp accepted")
	}
}

func TestTicketConcurrentConsumeExactlyOneWins(t *testing.T) {
	s := NewTicketStore()
	ticket, _ := s.Issue(testTicketActor(), time.Now().Add(time.Hour))
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, ok := s.Consume(ticket); ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("concurrent consume winners=%d, want exactly 1", got)
	}
}
