package storage

import "time"

// Event represents one lifecycle event of a proxied connection.
type Event struct {
	SessionID     string
	Timestamp     time.Time
	EventType     string
	Destination   string
	Transport     string
	Network       string
	LatencyMS     int64
	BytesSent     int64
	BytesReceived int64
	Status        string
	ErrorMessage  string
}

// Repository persists connection events.
type Repository interface {
	InsertEvent(event Event) error
	Close() error
}
