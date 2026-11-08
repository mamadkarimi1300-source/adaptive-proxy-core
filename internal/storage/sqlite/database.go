package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/storage"
)

// Store persists connection lifecycle events in a local SQLite database.
type Store struct {
	db *sql.DB
}

// Open creates the database directory, opens SQLite, and applies the schema.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("SQLite database path is required")
	}
	if path != ":memory:" && path != "file::memory:?cache=shared" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)

	store := &Store{db: db}
	if _, err := db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable SQLite WAL: %w", err)
	}
	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure SQLite busy timeout: %w", err)
	}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping SQLite database: %w", err)
	}
	return store, nil
}

func (s *Store) migrate() error {
	const schema = `
CREATE TABLE IF NOT EXISTS connection_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    timestamp DATETIME NOT NULL,
    event_type TEXT NOT NULL,
    destination TEXT,
    transport TEXT,
    network TEXT,
    latency_ms INTEGER NOT NULL DEFAULT 0,
    bytes_sent INTEGER NOT NULL DEFAULT 0,
    bytes_received INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_connection_logs_timestamp
    ON connection_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_connection_logs_session
    ON connection_logs(session_id);
CREATE INDEX IF NOT EXISTS idx_connection_logs_destination
    ON connection_logs(destination);
CREATE INDEX IF NOT EXISTS idx_connection_logs_event_type
    ON connection_logs(event_type);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("apply SQLite schema: %w", err)
	}
	return nil
}

// InsertEvent writes one event. A zero timestamp is replaced with the current UTC time.
func (s *Store) InsertEvent(event storage.Event) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	_, err := s.db.Exec(`
INSERT INTO connection_logs (
    session_id, timestamp, event_type, destination, transport, network,
    latency_ms, bytes_sent, bytes_received, status, error_message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.SessionID,
		event.Timestamp.UTC().Format(time.RFC3339Nano),
		event.EventType,
		nullable(event.Destination),
		nullable(event.Transport),
		nullable(event.Network),
		event.LatencyMS,
		event.BytesSent,
		event.BytesReceived,
		event.Status,
		nullable(event.ErrorMessage),
	)
	if err != nil {
		return fmt.Errorf("insert connection log: %w", err)
	}
	return nil
}

// Close releases the underlying SQLite connection pool.
func (s *Store) Close() error {
	return s.db.Close()
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
