package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/storage"
)

func TestStorePersistsConnectionEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	want := storage.Event{
		SessionID:   "session-1",
		Timestamp:   time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC),
		EventType:   "connection_established",
		Destination: "example.com:443",
		Transport:   "tcp",
		Network:     "tcp",
		LatencyMS:   42,
		Status:      "success",
	}
	if err := store.InsertEvent(want); err != nil {
		t.Fatalf("InsertEvent() error = %v", err)
	}

	var sessionID, eventType, destination, transport, status string
	var latency int64
	err = store.db.QueryRow(`
SELECT session_id, event_type, destination, transport, latency_ms, status
FROM connection_logs WHERE session_id = ?`, want.SessionID).Scan(
		&sessionID, &eventType, &destination, &transport, &latency, &status,
	)
	if err != nil {
		t.Fatalf("query event: %v", err)
	}
	if sessionID != want.SessionID || eventType != want.EventType ||
		destination != want.Destination || transport != want.Transport ||
		latency != want.LatencyMS || status != want.Status {
		t.Fatalf("stored event does not match: %q %q %q %q %d %q",
			sessionID, eventType, destination, transport, latency, status)
	}
}

func TestStoreCreatesSchemaOnExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open() on existing database: %v", err)
	}
	defer store.Close()

	var count int
	if err := store.db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master
WHERE type = 'table' AND name = 'connection_logs'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("connection_logs table count = %d, want 1", count)
	}
}
