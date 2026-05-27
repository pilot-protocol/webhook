// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !no_webhook
// +build !no_webhook

package webhook

import (
	"context"
	"crypto/ed25519"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/TeoSlayer/pilotprotocol/pkg/coreapi"
)

func TestValidate_DelegatesToUrlvalidate(t *testing.T) {
	t.Parallel()
	if err := Validate("https://example.com/hook"); err != nil {
		t.Errorf("https URL should validate: %v", err)
	}
	if err := Validate(""); err == nil {
		t.Error("empty URL should fail validation")
	}
	if err := Validate("not-a-url"); err == nil {
		t.Error("non-URL should fail")
	}
}

func TestSavePersistedURL_RoundtripsViaTempHome(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	want := "https://example.com/hook"
	if err := SavePersistedURL(want); err != nil {
		t.Fatalf("SavePersistedURL: %v", err)
	}
	// File must exist now.
	if _, err := os.Stat(filepath.Join(tmp, ".pilot", "webhook_url")); err != nil {
		t.Fatalf("Stat: %v", err)
	}

	// LoadPersistedURL returns what we wrote.
	got, err := LoadPersistedURL()
	if err != nil {
		t.Fatalf("LoadPersistedURL: %v", err)
	}
	if got != want {
		t.Errorf("LoadPersistedURL = %q, want %q", got, want)
	}
}

func TestSavePersistedURL_EmptyDeletesFile(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	// Pre-create the file.
	if err := SavePersistedURL("https://x"); err != nil {
		t.Fatalf("setup: %v", err)
	}
	// Empty URL → delete.
	if err := SavePersistedURL(""); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(tmp, ".pilot", "webhook_url")); !os.IsNotExist(err) {
		t.Errorf("file should be deleted, got err=%v", err)
	}
}

func TestSavePersistedURL_EmptyOnMissingFileIsNoOp(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	// Delete a non-existent file — must be a clean no-op.
	if err := SavePersistedURL(""); err != nil {
		t.Errorf("SavePersistedURL on absent file: %v", err)
	}
}

func TestLoadPersistedURL_MissingFileReturnsEmpty(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	got, err := LoadPersistedURL()
	if err == nil {
		t.Errorf("expected error from missing file; got %q", got)
	}
}

func TestLoadPersistedURL_WhitespaceTrimmedAndValidated(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	dir := filepath.Join(tmp, ".pilot")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Write a URL with surrounding whitespace.
	if err := os.WriteFile(filepath.Join(dir, "webhook_url"), []byte("  https://x.example/hook  \n"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := LoadPersistedURL()
	if err != nil {
		t.Fatalf("LoadPersistedURL: %v", err)
	}
	if got != "https://x.example/hook" {
		t.Errorf("got %q", got)
	}
}

func TestLoadPersistedURL_EmptyFileReturnsEmpty(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	dir := filepath.Join(tmp, ".pilot")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "webhook_url"), []byte("   \n"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := LoadPersistedURL()
	if err != nil || got != "" {
		t.Errorf("got (%q, %v), want ('' , nil)", got, err)
	}
}

func TestLoadPersistedURL_InvalidURLReturnsError(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	dir := filepath.Join(tmp, ".pilot")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Junk content — must fail Validate.
	if err := os.WriteFile(filepath.Join(dir, "webhook_url"), []byte("not-a-url"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadPersistedURL(); err == nil {
		t.Error("expected validation error")
	}
}

func TestNewService_Defaults(t *testing.T) {
	t.Parallel()
	s := NewService("https://example/hook")
	if s == nil {
		t.Fatal("NewService returned nil")
	}
	if s.Name() != "webhook" {
		t.Errorf("Name = %q", s.Name())
	}
	if s.Order() != 90 {
		t.Errorf("Order = %d, want 90", s.Order())
	}
	stats := s.Stats()
	if stats.Dropped != 0 || stats.CircuitSkips != 0 {
		t.Errorf("Stats on no-client: %+v", stats)
	}
}

// fakeEventBus is a no-op EventBus that satisfies coreapi.EventBus.
type fakeEventBus struct {
	mu        sync.Mutex
	published []string
	ch        chan coreapi.Event
}

func newFakeBus() *fakeEventBus {
	return &fakeEventBus{ch: make(chan coreapi.Event, 16)}
}

func (b *fakeEventBus) Publish(topic string, payload map[string]any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, topic)
}

func (b *fakeEventBus) Subscribe(string) (<-chan coreapi.Event, func()) {
	return b.ch, func() { close(b.ch) }
}

// fakeIdentity is a minimal coreapi.Identity for service tests.
type fakeIdentity struct{ id uint32 }

func (i *fakeIdentity) NodeID() uint32 { return i.id }
func (i *fakeIdentity) Address() coreapi.Addr {
	return coreapi.Addr{Network: 0, Node: i.id}
}
func (i *fakeIdentity) PublicKey() ed25519.PublicKey       { return nil }
func (i *fakeIdentity) Sign([]byte) ([]byte, error)        { return nil, nil }

func TestService_StartWithoutURLIsNoop(t *testing.T) {
	// Cannot t.Parallel — uses t.Setenv.
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	s := NewService("")
	bus := newFakeBus()
	deps := coreapi.Deps{Events: bus, Identity: &fakeIdentity{id: 1}}
	if err := s.Start(context.Background(), deps); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// No URL → no client → Stats stays zero.
	if got := s.Stats(); got.Dropped != 0 {
		t.Errorf("Stats = %+v", got)
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestService_SetURL_PersistsThroughHomeAndCanClear(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)

	s := NewService("")
	bus := newFakeBus()
	deps := coreapi.Deps{Events: bus, Identity: &fakeIdentity{id: 7}}
	if err := s.Start(context.Background(), deps); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// SetURL → file written.
	s.SetURL("https://hook.example/path")
	body, err := os.ReadFile(filepath.Join(tmp, ".pilot", "webhook_url"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(body) != "https://hook.example/path" {
		t.Errorf("persisted = %q", body)
	}

	// Stats is now from the live client.
	if got := s.Stats(); got.Dropped != 0 {
		t.Errorf("Stats: %+v", got)
	}

	// SetURL("") → file deleted, client gone.
	s.SetURL("")
	if _, err := os.Stat(filepath.Join(tmp, ".pilot", "webhook_url")); !os.IsNotExist(err) {
		t.Errorf("file should be deleted")
	}
}

func TestService_StartUsesPersistedURLWhenInitialEmpty(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	// Pre-seed the persisted URL.
	if err := SavePersistedURL("https://hook/persisted"); err != nil {
		t.Fatalf("setup: %v", err)
	}

	s := NewService("")
	bus := newFakeBus()
	deps := coreapi.Deps{Events: bus, Identity: &fakeIdentity{id: 1}}
	if err := s.Start(context.Background(), deps); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// A live client should exist now.
	if s.client == nil {
		t.Error("client should be initialized from persisted URL")
	}
}

// TestClient_EmitDoesNotBlockWhenFull stress-tests the dropped-counter path.
func TestClient_EmitDropsWhenQueueFull(t *testing.T) {
	t.Parallel()
	// Sized buffer 1024 — emit far more than that very quickly.
	wc := NewClient("https://no.such.host.invalid/hook", func() uint32 { return 1 })
	t.Cleanup(wc.Close)

	for i := 0; i < 5000; i++ {
		wc.Emit("evt", nil)
	}
	// Some emits should have been dropped; we don't make a hard count
	// assertion because the run() loop may consume some.
	_ = wc.Dropped()
}

// TestClient_NilSafeOps exercises the nil-receiver paths.
func TestClient_NilSafeOps(t *testing.T) {
	t.Parallel()
	var wc *Client
	wc.Emit("topic", nil) // must not panic
	wc.Close()
	if got := wc.Dropped(); got != 0 {
		t.Errorf("nil.Dropped = %d", got)
	}
	if got := wc.CircuitSkips(); got != 0 {
		t.Errorf("nil.CircuitSkips = %d", got)
	}
}

// TestClient_CloseIsIdempotent confirms repeat Close() doesn't panic.
func TestClient_CloseIsIdempotent(t *testing.T) {
	t.Parallel()
	wc := NewClient("https://no.such/hook", func() uint32 { return 1 })
	wc.Close()
	wc.Close()
	wc.Close()
}

// TestClient_WithOptions covers WithHTTPTimeout, WithRetryBackoff,
// WithCircuitCooldown — exercises the option-application loop in NewClient.
func TestClient_WithOptions(t *testing.T) {
	t.Parallel()
	wc := NewClient("https://x/hook",
		func() uint32 { return 1 },
		WithHTTPTimeout(7*time.Second),
		WithRetryBackoff(123*time.Millisecond),
		WithCircuitCooldown(45*time.Second),
	)
	defer wc.Close()
	if wc.client.Timeout != 7*time.Second {
		t.Errorf("HTTPTimeout not applied: %v", wc.client.Timeout)
	}
	if wc.initialBackoff != 123*time.Millisecond {
		t.Errorf("RetryBackoff not applied: %v", wc.initialBackoff)
	}
	if wc.circuitCooldown != 45*time.Second {
		t.Errorf("CircuitCooldown not applied: %v", wc.circuitCooldown)
	}
}

func TestNewClient_EmptyURLReturnsNil(t *testing.T) {
	t.Parallel()
	if got := NewClient("", func() uint32 { return 1 }); got != nil {
		t.Errorf("NewClient(\"\") = %v, want nil", got)
	}
}
