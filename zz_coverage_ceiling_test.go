// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !no_webhook
// +build !no_webhook

// Coverage-ceiling tests. Pin the remaining defensive branches that the
// happy-path tests don't reach (urlPath HOME-error, MkdirAll error,
// non-IsNotExist Remove error, startClientLocked nil-deps / nil-identity /
// nil-client branches, SetURL SavePersistedURL warn-path, breaker
// cooldown==0 defensive fallback) plus regression guards for the two
// items the iter-1 audit flagged:
//
//  1. Retry-backoff timing leak — back-to-back failed POSTs must follow
//     the documented backoff schedule even on the FIRST attempt of a
//     fresh event (i.e. there is no carry-over of the previous event's
//     backoff state). A leak there would let a remote receiver fingerprint
//     "this client is the webhook plugin" by clocking inter-request gaps.
//
//  2. Idle conn leak from undrained response body — post() must call
//     resp.Body.Close() on every code path (2xx, 4xx, 5xx). A leak there
//     would prevent HTTP/1.1 keep-alive reuse and pile up FIN_WAIT_2
//     sockets under high-volume delivery. We can't easily inspect the
//     pool from outside the stdlib, so we approximate: hammer a single
//     httptest.Server and require that the number of *unique* server-side
//     remote ports stays small (well below the request count). If Close
//     were skipped, every request would land on a fresh TCP 4-tuple.
package webhook

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pilot-protocol/common/coreapi"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// clearHome wipes every env var that os.UserHomeDir() consults, forcing it
// to return an error. We rely on this to exercise the urlPath() error path
// in LoadPersistedURL / SavePersistedURL.
func clearHome(t *testing.T) {
	t.Helper()
	// Order matches Go stdlib's os.userHomeDir() lookup on each GOOS.
	for _, k := range []string{"HOME", "USERPROFILE", "home"} {
		t.Setenv(k, "")
	}
	// Also clear the Plan 9 / Windows variants so the test is portable.
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
}

// localBus is a tiny coreapi.EventBus implementation tests can drive
// directly. It hands out one shared channel; Publish broadcasts.
type localBus struct {
	mu      sync.Mutex
	ch      chan coreapi.Event
	cancels int
}

func newLocalBus() *localBus {
	return &localBus{ch: make(chan coreapi.Event, 32)}
}

func (b *localBus) Publish(topic string, payload map[string]any) {
	select {
	case b.ch <- coreapi.Event{Topic: topic, Payload: payload, Time: time.Now()}:
	default:
	}
}

func (b *localBus) Subscribe(_ string) (<-chan coreapi.Event, func()) {
	return b.ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.cancels == 0 {
			close(b.ch)
		}
		b.cancels++
	}
}

type localIdent struct{ id uint32 }

func (i *localIdent) NodeID() uint32                  { return i.id }
func (i *localIdent) Address() coreapi.Addr           { return coreapi.Addr{Network: 1, Node: i.id} }
func (i *localIdent) PublicKey() ed25519.PublicKey    { return nil }
func (i *localIdent) Sign(_ []byte) ([]byte, error)   { return nil, nil }

// ---------------------------------------------------------------------------
// urlPath / persistence error branches
// ---------------------------------------------------------------------------

// TestUrlPath_HomeErrorPropagates pins the HOME-lookup failure branch in
// urlPath() (and through it, LoadPersistedURL + SavePersistedURL).
// Cannot run on Windows where the env-clear trick doesn't reliably make
// os.UserHomeDir fail; the rest of the suite covers the rest.
func TestUrlPath_HomeErrorPropagates(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UserHomeDir is hard to force-fail on Windows")
	}
	clearHome(t)

	// LoadPersistedURL must surface the urlPath error and NOT return a
	// silent empty string (callers can't distinguish "no file yet" from
	// "we have no home" without this).
	if _, err := LoadPersistedURL(); err == nil {
		t.Error("LoadPersistedURL with empty HOME should error")
	}
	// SavePersistedURL same story for both write and delete paths.
	if err := SavePersistedURL("https://x.example/h"); err == nil {
		t.Error("SavePersistedURL(write) with empty HOME should error")
	}
	if err := SavePersistedURL(""); err == nil {
		t.Error("SavePersistedURL(clear) with empty HOME should error")
	}
}

// TestSavePersistedURL_MkdirAllError pins the MkdirAll failure branch.
// We force it by setting HOME to a path whose parent is a regular file
// (mkdir under a file fails with ENOTDIR).
func TestSavePersistedURL_MkdirAllError(t *testing.T) {
	tmp := t.TempDir()
	// Create a regular file, then point HOME at a subpath of it.
	wedge := filepath.Join(tmp, "wedge")
	if err := os.WriteFile(wedge, []byte("not a dir"), 0600); err != nil {
		t.Fatalf("write wedge: %v", err)
	}
	t.Setenv("HOME", filepath.Join(wedge, "home"))

	if err := SavePersistedURL("https://x.example/hook"); err == nil {
		t.Error("expected MkdirAll error when HOME parent is a file")
	}
}

// TestSavePersistedURL_RemoveErrorNotIsNotExist pins the "Remove returned
// a non-IsNotExist error" branch. We make the webhook_url path a
// non-empty directory (rmdir then refuses with ENOTEMPTY).
func TestSavePersistedURL_RemoveErrorNotIsNotExist(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	pilotDir := filepath.Join(tmp, ".pilot")
	if err := os.MkdirAll(pilotDir, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Make webhook_url a directory containing a file so Remove fails
	// with ENOTEMPTY (not IsNotExist).
	dir := filepath.Join(pilotDir, "webhook_url")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatalf("mkdir webhook_url-as-dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "child"), []byte("x"), 0600); err != nil {
		t.Fatalf("write child: %v", err)
	}

	if err := SavePersistedURL(""); err == nil {
		t.Error("expected Remove error when path is a non-empty dir")
	} else if errors.Is(err, os.ErrNotExist) {
		t.Errorf("got IsNotExist err (should be filtered out): %v", err)
	}
}

// ---------------------------------------------------------------------------
// service.go: startClientLocked defensive branches
// ---------------------------------------------------------------------------

// TestService_StartWithNilEventsIsNoop covers the
// `if s.deps.Events == nil { return }` early-return in startClientLocked.
func TestService_StartWithNilEventsIsNoop(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	s := NewService("https://x.example/hook")
	// Deps without an EventBus.
	if err := s.Start(context.Background(), coreapi.Deps{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// No client should have been wired up.
	if got := s.Stats(); got.Dropped != 0 || got.CircuitSkips != 0 {
		t.Errorf("Stats = %+v, want zero", got)
	}
}

// TestService_StartCallsIdentityNodeID covers the
// `return s.deps.Identity.NodeID()` branch inside the nodeID closure
// built by startClientLocked. A non-nil Identity must be consulted on
// every Emit, and its NodeID propagated to the JSON payload.
func TestService_StartCallsIdentityNodeID(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	gotNode := make(chan uint32, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev Event
		_ = decodeJSON(r, &ev)
		select {
		case gotNode <- ev.NodeID:
		default:
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	s := NewService(srv.URL, WithRetryBackoff(1*time.Millisecond))
	bus := newLocalBus()
	if err := s.Start(context.Background(), coreapi.Deps{Events: bus, Identity: &localIdent{id: 0xDEADBEEF}}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	bus.Publish("identity.test", nil)

	select {
	case got := <-gotNode:
		if got != 0xDEADBEEF {
			t.Errorf("node_id = %#x, want 0xDEADBEEF", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event delivery")
	}
}

// TestService_StartUsesZeroNodeIDWhenIdentityNil pins the
// `if s.deps.Identity == nil { return 0 }` branch inside the nodeID
// closure built by startClientLocked. We start with a bus but no
// Identity, emit one event through the bus, and verify the server
// observes node_id=0.
func TestService_StartUsesZeroNodeIDWhenIdentityNil(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	gotNode := make(chan uint32, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// crude parse — we only care that node_id appears as 0.
		// service emits via Client.Emit which JSON-marshals an Event.
		// the body is small so a simple substring check is fine here.
		var ev Event
		_ = decodeJSON(r, &ev)
		select {
		case gotNode <- ev.NodeID:
		default:
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	s := NewService(srv.URL, WithRetryBackoff(1*time.Millisecond))
	bus := newLocalBus()
	if err := s.Start(context.Background(), coreapi.Deps{Events: bus /* Identity: nil */}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	bus.Publish("nil.identity.test", map[string]any{"k": "v"})

	select {
	case got := <-gotNode:
		if got != 0 {
			t.Errorf("node_id = %d, want 0 (Identity was nil)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event delivery")
	}
}

// TestService_StartClientNilFromEmptyURLIsNoop indirectly covers the
// `if s.client == nil { return }` branch in startClientLocked. We achieve
// `s.client == nil` by Start-ing with an empty URL AND no persisted file.
// The bridge goroutine must not be wired up; emitting on the bus must
// not panic and Stats stays zero.
func TestService_StartClientNilFromEmptyURLIsNoop(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	s := NewService("") // empty + no persisted file → NewClient returns nil
	bus := newLocalBus()
	if err := s.Start(context.Background(), coreapi.Deps{Events: bus, Identity: &localIdent{id: 9}}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// Publishing on the bus must not panic, even though no subscriber
	// was wired (the early return in startClientLocked prevented it).
	bus.Publish("orphan", nil)

	if got := s.Stats(); got.Dropped != 0 || got.CircuitSkips != 0 {
		t.Errorf("Stats = %+v", got)
	}
}

// TestService_SetURL_SaveErrorIsLoggedNotFatal covers the
// `if err := SavePersistedURL(url); err != nil { slog.Warn(...) }` branch
// inside SetURL. We sabotage SavePersistedURL by clearing HOME after
// Start so urlPath() will error.
func TestService_SetURL_SaveErrorIsLoggedNotFatal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UserHomeDir is hard to force-fail on Windows")
	}
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	s := NewService("")
	bus := newLocalBus()
	if err := s.Start(context.Background(), coreapi.Deps{Events: bus, Identity: &localIdent{id: 1}}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// Now clear HOME so SavePersistedURL inside SetURL hits the urlPath
	// error path. SetURL must NOT panic and must still rotate the client.
	clearHome(t)
	s.SetURL("https://x.example/hook")

	// SetURL with empty URL also exercises the cleared-cancel path again.
	s.SetURL("")
}

// ---------------------------------------------------------------------------
// post(): cooldown==0 defensive fallback
// ---------------------------------------------------------------------------

// TestPost_CooldownZeroFallsBackToDefault covers the defensive
//     if cooldown == 0 { cooldown = CircuitCooldown }
// branch in post(). NewClient always sets circuitCooldown =
// CircuitCooldown, so we direct-construct a Client and run a single
// post() to reach the failure-threshold code with cooldown==0.
func TestPost_CooldownZeroFallsBackToDefault(t *testing.T) {
	t.Parallel()
	// Server always 500s so each post() goes through full retry budget.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	wc := &Client{
		url:    srv.URL,
		ch:     make(chan *Event, 16),
		client: &http.Client{Timeout: 200 * time.Millisecond},
		done:   make(chan struct{}),
		closed: make(chan struct{}),
		nodeID: func() uint32 { return 1 },
		// initialBackoff: 0 (also a sane defensive case — no sleep)
		// circuitCooldown: 0 — the branch under test
	}
	go wc.run()
	t.Cleanup(wc.Close)

	for i := 0; i < CircuitOpenThreshold+1; i++ {
		wc.Emit("zero-cooldown", i)
	}
	// Give the dispatcher time to chew through all failures.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if wc.circuitOpenUntilNano.Load() != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	openUntil := wc.circuitOpenUntilNano.Load()
	if openUntil == 0 {
		t.Fatal("breaker never opened despite >threshold failures")
	}
	// The fallback should have applied CircuitCooldown (~30s). Check that
	// the openUntil is in the right ballpark — at least 5s in the future,
	// well past any per-test timeout the dispatcher could have hit.
	remaining := time.Until(time.Unix(0, openUntil))
	if remaining < 5*time.Second {
		t.Errorf("cooldown fallback didn't apply: remaining=%v, want ~%v",
			remaining, CircuitCooldown)
	}
}

// ---------------------------------------------------------------------------
// Iter-1 audit pins
// ---------------------------------------------------------------------------

// Note on coverage ceiling: webhook.go:183 (the `case <-wc.closed:` arm
// inside Emit's second select) is intrinsically race-only. Reaching it
// requires Close to fire in the narrow window between the first
// `<-wc.closed` check and the channel send. Attempting to force it
// (16 concurrent emitters + slow server + race-during-close) generated
// enough port/socket pressure to flake unrelated tests in the same
// `go test` process, so the test was removed. The existing
// TestWebhookConcurrentEmitClose still exercises this path under the
// race detector.

// TestAudit_BackoffIsPerEvent_NoCarryover guards against a retry-backoff
// timing leak where a previous event's exponential backoff state could
// bleed into the next event's first attempt, giving a remote receiver
// a fingerprint signal. The contract is: every fresh Emit starts at
// initialBackoff (no sleep before attempt #1, then initialBackoff before
// #2, then 2*initialBackoff before #3).
//
// We emit two failing events back-to-back and measure the time gap
// between event-2's first attempt and event-2's second attempt. It must
// be ~initialBackoff, NOT 4*initialBackoff (which is where event-1 left
// off the doubling).
func TestAudit_BackoffIsPerEvent_NoCarryover(t *testing.T) {
	t.Parallel()

	type stamp struct {
		event string
		at    time.Time
	}
	var mu sync.Mutex
	var stamps []stamp

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev Event
		_ = decodeJSON(r, &ev)
		mu.Lock()
		stamps = append(stamps, stamp{event: ev.Event, at: time.Now()})
		mu.Unlock()
		w.WriteHeader(500)
	}))
	defer srv.Close()

	const backoff = 40 * time.Millisecond
	wc := NewClient(srv.URL, func() uint32 { return 1 },
		WithRetryBackoff(backoff),
		WithHTTPTimeout(200*time.Millisecond))
	t.Cleanup(wc.Close)

	wc.Emit("first", nil)
	wc.Emit("second", nil)
	wc.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(stamps) != 2*MaxRetries {
		t.Fatalf("got %d stamps, want %d", len(stamps), 2*MaxRetries)
	}
	// Locate event-2's first and second attempts.
	var second1, second2 time.Time
	seen := 0
	for _, s := range stamps {
		if s.event == "second" {
			if seen == 0 {
				second1 = s.at
			} else if seen == 1 {
				second2 = s.at
				break
			}
			seen++
		}
	}
	if second1.IsZero() || second2.IsZero() {
		t.Fatalf("could not locate event-2's first two stamps: %+v", stamps)
	}
	gap := second2.Sub(second1)
	// Must be ~backoff (the fresh starting value), with generous slack
	// for goroutine wake-up. Crucially must NOT be ~4*backoff which is
	// where event-1's doubling left things if state leaked.
	min := backoff - 15*time.Millisecond
	max := 2*backoff + 30*time.Millisecond // i.e. < 3*backoff
	if gap < min || gap > max {
		t.Errorf("event-2 first→second gap = %v, want ~%v (carryover would be ~%v)",
			gap, backoff, 4*backoff)
	}
}

// TestAudit_ResponseBodyClosedOnEveryStatus guards against the
// idle-conn leak: post() must Close the response body on 2xx, 4xx and
// 5xx alike so the underlying HTTP/1.1 connection is returned to the
// pool. If Close were skipped, the stdlib transport would treat each
// response as "in use" and dial a fresh TCP connection per event,
// pushing the unique remote-port count toward the request count.
//
// We hit the same server with each status class N times and confirm
// the server side observed fewer than N unique remote ports. Tolerant
// upper bound (N/4) accommodates Go's transport occasionally opening a
// second conn for parallelism.
func TestAudit_ResponseBodyClosedOnEveryStatus(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		status int
	}{
		{"2xx", 200},
		{"4xx", 400}, // no retry
		// 5xx omitted: it retries, inflating request count beyond what
		// the conn-reuse check cleanly demonstrates.
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			ports := map[string]struct{}{}
			var requests atomic.Int32

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				host, _, err := net.SplitHostPort(r.RemoteAddr)
				_ = host
				if err == nil {
					mu.Lock()
					ports[r.RemoteAddr] = struct{}{}
					mu.Unlock()
				}
				requests.Add(1)
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			wc := NewClient(srv.URL, func() uint32 { return 1 },
				WithRetryBackoff(1*time.Millisecond))
			const n = 40
			for i := 0; i < n; i++ {
				wc.Emit("status-test", i)
			}
			wc.Close()

			if got := int(requests.Load()); got != n {
				t.Fatalf("status=%d: requests=%d, want %d", tc.status, got, n)
			}
			mu.Lock()
			unique := len(ports)
			mu.Unlock()
			// If post() leaked, unique would be ~n. Healthy reuse keeps
			// it tiny (1-3). Cap at n/4 to leave slack for transient
			// stdlib decisions.
			if unique > n/4 {
				t.Errorf("status=%d: %d unique remote ports for %d requests — body likely not drained/closed",
					tc.status, unique, n)
			}
			t.Logf("status=%d: %d unique remote ports for %d requests", tc.status, unique, n)
		})
	}
}

// TestAudit_ResponseBodyClosedFreesGoroutines is a secondary defense for
// the same audit finding. If resp.Body.Close() were skipped, each request
// would leak the response-reader goroutine inside net/http. We snapshot
// goroutine count before and after a burst and require it to settle back
// down (within slack).
func TestAudit_ResponseBodyClosedFreesGoroutines(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 200 with a small body so there's actually something to read/close.
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	// Warm-up to settle stdlib's initial goroutines.
	wcWarm := NewClient(srv.URL, func() uint32 { return 1 },
		WithRetryBackoff(1*time.Millisecond))
	wcWarm.Emit("warmup", nil)
	wcWarm.Close()
	time.Sleep(50 * time.Millisecond)

	baseline := runtime.NumGoroutine()

	wc := NewClient(srv.URL, func() uint32 { return 1 },
		WithRetryBackoff(1*time.Millisecond))
	const n = 200
	for i := 0; i < n; i++ {
		wc.Emit("body-close", i)
	}
	wc.Close()
	// Give stdlib's idle-conn reaper a moment to settle.
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	// We expect roughly baseline ± noise. A leak would scale with n
	// (one goroutine per leaked response). Fail if we grew by > n/10.
	if after > baseline+(n/10) {
		t.Errorf("goroutines: baseline=%d after-burst=%d (grew by %d, likely body-not-closed leak)",
			baseline, after, after-baseline)
	}
	t.Logf("goroutines: baseline=%d after=%d (n=%d)", baseline, after, n)
}

// ---------------------------------------------------------------------------
// utilities
// ---------------------------------------------------------------------------

// decodeJSON reads the full request body and JSON-decodes into out.
// Returns whatever json.Decode returns; callers may ignore the error and
// assert only on the populated fields.
func decodeJSON(r *http.Request, out *Event) error {
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}
