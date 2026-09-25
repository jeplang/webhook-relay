//go:build integration

package store_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"webhook-relay/internal/store"
)

// Tests here run against a real Postgres (default: the local compose-less
// container, or $RELAY_DATABASE_URL). Skipped automatically in CI unless the
// tag is passed.

func TestIntegration(t *testing.T) {
	dsn := os.Getenv("RELAY_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://relay:relay@localhost:5432/relay?sslmode=disable"
	}
	ctx := context.Background()

	// Reset schema from the migration file.
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	defer raw.Close()
	if _, err := raw.ExecContext(ctx, `DROP TABLE IF EXISTS deliveries, events, subscriptions CASCADE`); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	mig, err := os.ReadFile("../../migrations/0001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := raw.ExecContext(ctx, string(mig)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}

	st, err := store.NewSQLStore(dsn)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer st.Close()
	if err := st.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	t.Run("enqueue fan-out and 409", func(t *testing.T) {
		all, err := st.CreateSubscription(ctx, store.Subscription{
			URL: "https://all.example/h", Secret: "aa", EventTypes: []string{},
		})
		if err != nil {
			t.Fatalf("create all-sub: %v", err)
		}
		if all.ID == "" {
			t.Fatal("generated ID is empty")
		}
		if _, err := st.CreateSubscription(ctx, store.Subscription{
			URL: "https://filtered.example/h", Secret: "bb", EventTypes: []string{"payment.paid"},
		}); err != nil {
			t.Fatalf("create filtered-sub: %v", err)
		}

		ev := store.Event{
			ID: "itest-evt-1", Type: "payment.paid", Source: "billing",
			Payload: `{"amount":4200}`,
		}
		queued, err := st.InsertEventAndDeliveries(ctx, ev)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		if queued != 2 {
			t.Fatalf("queued = %d, want 2", queued)
		}

		// Duplicate → ErrEventExists.
		if _, err := st.InsertEventAndDeliveries(ctx, ev); err != store.ErrEventExists {
			t.Fatalf("duplicate insert err = %v, want ErrEventExists", err)
		}

		got, err := st.GetEvent(ctx, "itest-evt-1")
		if err != nil {
			t.Fatalf("get event: %v", err)
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(got.Payload), &data); err != nil {
			t.Fatalf("payload round-trip: %v", err)
		}
		if data["amount"].(float64) != 4200 {
			t.Errorf("payload amount = %v, want 4200", data["amount"])
		}

		subs, err := st.ListSubscriptions(ctx)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		if len(subs) != 2 {
			t.Fatalf("list len = %d, want 2", len(subs))
		}
		for _, s := range subs {
			if s.Secret != "" {
				t.Errorf("list leaked secret for %s", s.ID)
			}
		}
	})

	t.Run("claim deliver dead and reschedule", func(t *testing.T) {
		claimed, err := st.ClaimDeliveries(ctx, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if len(claimed) != 2 {
			t.Fatalf("claimed = %d, want 2", len(claimed))
		}
		for i := range claimed {
			c := &claimed[i]
			if c.EventPayload == "" || c.SubURL == "" || c.SubSecret == "" {
				t.Fatalf("claim %d missing details: %+v", i, c)
			}
			if c.Attempts != 1 {
				t.Fatalf("attempts = %d, want 1 (post-claim)", c.Attempts)
			}
		}

		// One delivered, one dead-lettered (4xx → permanent).
		if err := st.MarkDelivered(ctx, claimed[0].ID); err != nil {
			t.Fatalf("mark delivered: %v", err)
		}
		if err := st.MarkDead(ctx, claimed[1].ID, 404, "not found"); err != nil {
			t.Fatalf("mark dead: %v", err)
		}

		// Nothing left to claim.
		again, err := st.ClaimDeliveries(ctx, 10)
		if err != nil {
			t.Fatalf("re-claim: %v", err)
		}
		if len(again) != 0 {
			t.Fatalf("re-claim = %d rows, want 0", len(again))
		}

		// Reschedule path: new event, claim, reschedule due-now, claim again.
		queued, err := st.InsertEventAndDeliveries(ctx, store.Event{
			ID: "itest-evt-2", Type: "refund.issued", Source: "billing", Payload: `{}`,
		})
		if err != nil {
			t.Fatalf("insert evt2: %v", err)
		}
		if queued != 1 { // only the all-type sub matches
			t.Fatalf("queued = %d, want 1", queued)
		}
		c2, err := st.ClaimDeliveries(ctx, 10)
		if err != nil || len(c2) != 1 {
			t.Fatalf("claim evt2: n=%d err=%v", len(c2), err)
		}
		if err := st.Reschedule(ctx, c2[0].ID, time.Now(), nil, nil); err != nil {
			t.Fatalf("reschedule: %v", err)
		}
		c3, err := st.ClaimDeliveries(ctx, 10)
		if err != nil || len(c3) != 1 {
			t.Fatalf("re-claim evt2: n=%d err=%v", len(c3), err)
		}
		if c3[0].Attempts != 2 {
			t.Fatalf("attempts = %d, want 2", c3[0].Attempts)
		}
		_ = st.MarkDelivered(ctx, c3[0].ID)
	})

	t.Run("stale recovery", func(t *testing.T) {
		// payment.paid matches both subs (all-type + filtered) → 2 deliveries.
		queued, err := st.InsertEventAndDeliveries(ctx, store.Event{
			ID: "itest-evt-3", Type: "payment.paid", Source: "billing", Payload: `{}`,
		})
		if err != nil {
			t.Fatalf("insert evt3: %v", err)
		}
		if queued != 2 {
			t.Fatalf("queued = %d, want 2", queued)
		}
		c, err := st.ClaimDeliveries(ctx, 10)
		if err != nil || len(c) != 2 {
			t.Fatalf("claim evt3: n=%d err=%v", len(c), err)
		}
		// Age both in_flight rows beyond the 5m stale window.
		for i := range c {
			if _, err := raw.ExecContext(ctx,
				`UPDATE deliveries SET last_attempt_at = now() - interval '1 hour' WHERE id = $1`, c[i].ID); err != nil {
				t.Fatalf("age row: %v", err)
			}
		}
		n, err := st.RequeueStale(ctx, 5*time.Minute)
		if err != nil {
			t.Fatalf("requeue stale: %v", err)
		}
		if n != 2 {
			t.Fatalf("re-queued = %d, want 2", n)
		}
		// Backoff for attempts=1 is 1 minute: next_attempt_at must be future,
		// within now+2m, for both rows.
		rows, err := raw.QueryContext(ctx,
			`SELECT next_attempt_at FROM deliveries WHERE event_id = 'itest-evt-3'`)
		if err != nil {
			t.Fatalf("query next: %v", err)
		}
		defer rows.Close()
		checked := 0
		for rows.Next() {
			var next time.Time
			if err := rows.Scan(&next); err != nil {
				t.Fatalf("scan next: %v", err)
			}
			if !next.After(time.Now()) || next.Before(time.Now().Add(45*time.Second)) {
				t.Fatalf("next_attempt_at = %v, want now+~1m", next)
			}
			checked++
		}
		if checked != 2 {
			t.Fatalf("rows checked = %d, want 2", checked)
		}
		if due, err := st.CountDue(ctx); err != nil || due != 0 {
			t.Fatalf("count due = %d err=%v, want 0 (backoff in future)", due, err)
		}
	})

	t.Run("delete subscription cascades", func(t *testing.T) {
		s, err := st.CreateSubscription(ctx, store.Subscription{URL: "https://gone.example/h", Secret: "cc"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.InsertEventAndDeliveries(ctx, store.Event{
			ID: "itest-evt-4", Type: "x.y", Source: "billing", Payload: `{}`,
		}); err != nil {
			t.Fatal(err)
		}
		if err := st.DeleteSubscription(ctx, s.ID); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := st.DeleteSubscription(ctx, s.ID); err != store.ErrNotFound {
			t.Fatalf("repeat delete err = %v, want ErrNotFound", err)
		}
		var remaining int
		if err := raw.QueryRowContext(ctx,
			`SELECT count(*) FROM deliveries WHERE subscription_id = $1`, s.ID).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining != 0 {
			t.Fatalf("deliveries after delete = %d, want 0 (cascade)", remaining)
		}
	})

	t.Run("readyz ping", func(t *testing.T) {
		if err := st.Ping(ctx); err != nil {
			t.Fatalf("ping: %v", err)
		}
	})
}
