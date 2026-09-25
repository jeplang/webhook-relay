// Package store defines the persistence interface for webhook-relay plus its
// Postgres implementation (sql.go) and in-memory fake (fake.go).
package store

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors.
var (
	// ErrEventExists is returned when an event ID was already ingested.
	ErrEventExists = errors.New("store: event already exists")
	// ErrNotFound is returned for unknown IDs.
	ErrNotFound = errors.New("store: not found")
)

// Event is a producer event. Payload is the producer's JSON data as a string.
type Event struct {
	ID        string
	Type      string
	Source    string
	Payload   string
	CreatedAt time.Time
}

// Subscription is a webhook endpoint. Secret is hex-encoded and only ever
// returned to the caller at creation time.
type Subscription struct {
	ID         string
	URL        string
	Secret     string
	EventTypes []string // empty = receive all types
	CreatedAt  time.Time
}

// Delivery is one (event, subscription) fan-out row — the idempotency anchor.
type Delivery struct {
	ID             string
	EventID        string
	SubscriptionID string
	Status         string // pending | in_flight | delivered | dead
	Attempts       int
	NextAttemptAt  time.Time
	LastAttemptAt  *time.Time
	LastHTTPStatus *int
	LastError      *string
	DeliveredAt    *time.Time
}

// ClaimedDelivery is everything the worker needs to sign and send a delivery
// without re-querying. Attempts is the post-claim value.
type ClaimedDelivery struct {
	ID           string
	EventID      string
	EventType    string
	EventPayload string
	SubURL       string
	SubSecret    string
	Attempts     int
}

// Store is the persistence contract.
type Store interface {
	// InsertEventAndDeliveries is the transactional outbox write: insert the
	// event and one delivery row per matching active subscription in a single
	// transaction. Returns ErrEventExists if the event ID is already known.
	// Matching rule: subscriptions whose event_types is empty or contains ev.Type.
	InsertEventAndDeliveries(ctx context.Context, ev Event) (queued int, err error)

	// GetEvent returns an event by ID (ErrNotFound if unknown).
	GetEvent(ctx context.Context, id string) (Event, error)

	// CreateSubscription stores a new subscription and returns it, including
	// the generated ID and creation time.
	CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error)

	// ListSubscriptions returns subscriptions without their secrets.
	ListSubscriptions(ctx context.Context) ([]Subscription, error)

	// DeleteSubscription hard-deletes a subscription; its delivery rows
	// cascade. Returns ErrNotFound for unknown IDs.
	DeleteSubscription(ctx context.Context, id string) error

	// ClaimDeliveries atomically claims up to limit due pending deliveries
	// (FOR UPDATE SKIP LOCKED), flips them to in_flight, increments attempts,
	// and returns them with their event/subscription details.
	ClaimDeliveries(ctx context.Context, limit int) ([]ClaimedDelivery, error)

	// MarkDelivered marks a claimed delivery delivered.
	MarkDelivered(ctx context.Context, id string) error

	// MarkDead marks a claimed delivery permanently failed (dead-letter).
	MarkDead(ctx context.Context, id string, httpStatus int, errMsg string) error

	// Reschedule re-queues a failed delivery with a future next_attempt_at.
	// httpStatus and errMsg may be nil (e.g. network errors have no status).
	Reschedule(ctx context.Context, id string, nextAttempt time.Time, httpStatus *int, errMsg *string) error

	// RequeueStale flips in_flight rows whose last_attempt_at is older than
	// olderThan back to pending, with backoff based on their attempt count.
	// Returns the number of re-queued rows.
	RequeueStale(ctx context.Context, olderThan time.Duration) (int, error)

	// CountDue returns how many pending deliveries are due now.
	CountDue(ctx context.Context) (int, error)

	// Ping reports database reachability (used by /readyz).
	Ping(ctx context.Context) error
}
