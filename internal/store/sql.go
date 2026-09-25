package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// SQLStore implements Store on PostgreSQL via database/sql + pgx stdlib.
type SQLStore struct {
	db *sql.DB
}

// NewSQLStore opens the database connection pool for dsn.
func NewSQLStore(dsn string) (*SQLStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(time.Hour)
	return &SQLStore{db: db}, nil
}

// Close releases the connection pool.
func (s *SQLStore) Close() error { return s.db.Close() }

// Ping implements Store.
func (s *SQLStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// InsertEventAndDeliveries implements the transactional outbox write:
// event row + one delivery row per matching subscription, one transaction.
func (s *SQLStore) InsertEventAndDeliveries(ctx context.Context, ev Event) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO events (id, type, source, payload)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO NOTHING`,
		ev.ID, ev.Type, ev.Source, ev.Payload)
	if err != nil {
		return 0, fmt.Errorf("store: insert event: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return 0, ErrEventExists
	}

	// Fan out to matching subscriptions: empty event_types receives all.
	res, err = tx.ExecContext(ctx,
		`INSERT INTO deliveries (event_id, subscription_id)
		 SELECT $1, id FROM subscriptions
		 WHERE cardinality(event_types) = 0 OR $2 = ANY(event_types)`,
		ev.ID, ev.Type)
	if err != nil {
		return 0, fmt.Errorf("store: insert deliveries: %w", err)
	}
	queued, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: delivery count: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("store: commit: %w", err)
	}
	return int(queued), nil
}

// GetEvent implements Store.
func (s *SQLStore) GetEvent(ctx context.Context, id string) (Event, error) {
	var e Event
	err := s.db.QueryRowContext(ctx,
		`SELECT id, type, source, payload, created_at FROM events WHERE id = $1`, id,
	).Scan(&e.ID, &e.Type, &e.Source, &e.Payload, &e.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return e, ErrNotFound
	}
	if err != nil {
		return e, fmt.Errorf("store: get event: %w", err)
	}
	return e, nil
}

// CreateSubscription implements Store.
func (s *SQLStore) CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error) {
	// Ensure a non-nil slice so Postgres stores '{}' instead of NULL.
	if sub.EventTypes == nil {
		sub.EventTypes = []string{}
	}
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO subscriptions (url, secret, event_types)
		 VALUES ($1, $2, $3)
		 RETURNING id, created_at`,
		sub.URL, sub.Secret, sub.EventTypes,
	).Scan(&sub.ID, &sub.CreatedAt)
	if err != nil {
		return sub, fmt.Errorf("store: create subscription: %w", err)
	}
	return sub, nil
}

// ListSubscriptions implements Store. Secrets are never returned.
func (s *SQLStore) ListSubscriptions(ctx context.Context) ([]Subscription, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, url, event_types, created_at
		 FROM subscriptions ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("store: list subscriptions: %w", err)
	}
	defer rows.Close()

	var subs []Subscription
	for rows.Next() {
		var sub Subscription
		if err := rows.Scan(&sub.ID, &sub.URL, &sub.EventTypes, &sub.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: scan subscription: %w", err)
		}
		subs = append(subs, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list subscriptions: %w", err)
	}
	return subs, nil
}

// DeleteSubscription implements Store.
func (s *SQLStore) DeleteSubscription(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM subscriptions WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("store: delete subscription: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// ClaimDeliveries implements Store. See PROJECT_BRIEF.md §7 for the claim
// query; attempts is incremented at claim time so a crash-loop cannot dodge
// the dead-letter budget.
func (s *SQLStore) ClaimDeliveries(ctx context.Context, limit int) ([]ClaimedDelivery, error) {
	rows, err := s.db.QueryContext(ctx,
		`UPDATE deliveries d
		 SET status = 'in_flight', attempts = attempts + 1, last_attempt_at = now()
		 WHERE d.id IN (
		     SELECT id FROM deliveries
		     WHERE status = 'pending' AND next_attempt_at <= now()
		     ORDER BY next_attempt_at
		     FOR UPDATE SKIP LOCKED
		     LIMIT $1
		 )
		 RETURNING d.id, d.attempts`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	defer rows.Close()

	type claimed struct {
		id       string
		attempts int
	}
	var ids []string
	for rows.Next() {
		var c claimed
		if err := rows.Scan(&c.id, &c.attempts); err != nil {
			return nil, fmt.Errorf("store: claim scan: %w", err)
		}
		ids = append(ids, c.id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	// Fetch event + subscription details for the claimed rows in one query.
	detailRows, err := s.db.QueryContext(ctx,
		`SELECT dl.id, dl.event_id, e.type, e.payload, s.url, s.secret, dl.attempts
		 FROM deliveries dl
		 JOIN events e        ON e.id = dl.event_id
		 JOIN subscriptions s ON s.id = dl.subscription_id
		 WHERE dl.id = ANY($1::text[]::uuid[])`, ids)
	if err != nil {
		return nil, fmt.Errorf("store: claim details: %w", err)
	}
	defer detailRows.Close()

	var out []ClaimedDelivery
	for detailRows.Next() {
		var cd ClaimedDelivery
		if err := detailRows.Scan(&cd.ID, &cd.EventID, &cd.EventType, &cd.EventPayload,
			&cd.SubURL, &cd.SubSecret, &cd.Attempts); err != nil {
			return nil, fmt.Errorf("store: claim detail scan: %w", err)
		}
		out = append(out, cd)
	}
	if err := detailRows.Err(); err != nil {
		return nil, fmt.Errorf("store: claim details: %w", err)
	}
	return out, nil
}

// MarkDelivered implements Store.
func (s *SQLStore) MarkDelivered(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET status = 'delivered', delivered_at = now() WHERE id = $1`, id); err != nil {
		return fmt.Errorf("store: mark delivered: %w", err)
	}
	return nil
}

// MarkDead implements Store.
func (s *SQLStore) MarkDead(ctx context.Context, id string, httpStatus int, errMsg string) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE deliveries SET status = 'dead', last_http_status = $2, last_error = $3 WHERE id = $1`,
		id, httpStatus, errMsg); err != nil {
		return fmt.Errorf("store: mark dead: %w", err)
	}
	return nil
}

// Reschedule implements Store.
func (s *SQLStore) Reschedule(ctx context.Context, id string, nextAttempt time.Time, httpStatus *int, errMsg *string) error {
	if _, err := s.db.ExecContext(ctx,
		`UPDATE deliveries
		 SET status = 'pending', next_attempt_at = $2, last_http_status = $3, last_error = $4
		 WHERE id = $1`,
		id, nextAttempt, httpStatus, errMsg); err != nil {
		return fmt.Errorf("store: reschedule: %w", err)
	}
	return nil
}

// RequeueStale implements Store. Backoff mirrors the retry policy
// (PROJECT_BRIEF.md §8.2) keyed off the claim-time attempt count.
func (s *SQLStore) RequeueStale(ctx context.Context, olderThan time.Duration) (int, error) {
	// Pass the window as integer seconds: Go's Duration.String() ("5m0s")
	// does not reliably type as a Postgres interval parameter.
	res, err := s.db.ExecContext(ctx,
		`UPDATE deliveries
		 SET status = 'pending',
		     next_attempt_at = CASE attempts
		         WHEN 1 THEN now() + interval '1 minute'
		         WHEN 2 THEN now() + interval '5 minutes'
		         WHEN 3 THEN now() + interval '30 minutes'
		         ELSE now() + interval '2 hours'
		     END
		 WHERE status = 'in_flight' AND last_attempt_at < now() - ($1 * interval '1 second')`,
		int(olderThan.Seconds()))
	if err != nil {
		return 0, fmt.Errorf("store: requeue stale: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: requeue stale count: %w", err)
	}
	return int(n), nil
}

// CountDue implements Store.
func (s *SQLStore) CountDue(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM deliveries WHERE status = 'pending' AND next_attempt_at <= now()`).
		Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: count due: %w", err)
	}
	return n, nil
}
