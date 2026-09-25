package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Fake is an in-memory Store for unit tests. Safe for concurrent use.
// Now is injectable so tests can control due-ness without sleeping.
type Fake struct {
	mu     sync.Mutex
	events map[string]Event
	subs   map[string]Subscription
	dels   map[string]Delivery
	seq    int

	// Now controls the fake clock.
	Now func() time.Time

	// PingErr, when set, makes Ping return it (for /readyz failure tests).
	PingErr error
}

// NewFake returns an empty Fake with a wall-clock Now.
func NewFake() *Fake {
	return &Fake{
		events: map[string]Event{},
		subs:   map[string]Subscription{},
		dels:   map[string]Delivery{},
		Now:    time.Now,
	}
}

// InsertEventAndDeliveries implements Store.
func (f *Fake) InsertEventAndDeliveries(_ context.Context, ev Event) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.events[ev.ID]; ok {
		return 0, ErrEventExists
	}
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = f.Now()
	}
	f.events[ev.ID] = ev

	queued := 0
	for _, s := range f.subs {
		if !matchesType(s.EventTypes, ev.Type) {
			continue
		}
		f.seq++
		id := fmt.Sprintf("del-%d", f.seq)
		f.dels[id] = Delivery{
			ID:             id,
			EventID:        ev.ID,
			SubscriptionID: s.ID,
			Status:         "pending",
			NextAttemptAt:  f.Now(),
		}
		queued++
	}
	return queued, nil
}

// GetEvent implements Store.
func (f *Fake) GetEvent(_ context.Context, id string) (Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.events[id]
	if !ok {
		return Event{}, ErrNotFound
	}
	return e, nil
}

// CreateSubscription implements Store.
func (f *Fake) CreateSubscription(_ context.Context, sub Subscription) (Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if sub.ID == "" {
		f.seq++
		sub.ID = fmt.Sprintf("sub-%d", f.seq)
	}
	if sub.EventTypes == nil {
		sub.EventTypes = []string{}
	}
	if sub.CreatedAt.IsZero() {
		sub.CreatedAt = f.Now()
	}
	f.subs[sub.ID] = sub
	return sub, nil
}

// ListSubscriptions implements Store (secrets stripped).
func (f *Fake) ListSubscriptions(_ context.Context) ([]Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var subs []Subscription
	for _, s := range f.subs {
		s.Secret = ""
		subs = append(subs, s)
	}
	sort.Slice(subs, func(i, j int) bool { return subs[i].CreatedAt.Before(subs[j].CreatedAt) })
	return subs, nil
}

// DeleteSubscription implements Store.
func (f *Fake) DeleteSubscription(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.subs[id]; !ok {
		return ErrNotFound
	}
	delete(f.subs, id)
	for dID, d := range f.dels {
		if d.SubscriptionID == id {
			delete(f.dels, dID)
		}
	}
	return nil
}

// ClaimDeliveries implements Store.
func (f *Fake) ClaimDeliveries(_ context.Context, limit int) ([]ClaimedDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.Now()
	var due []Delivery
	for _, d := range f.dels {
		if d.Status == "pending" && !d.NextAttemptAt.After(now) {
			due = append(due, d)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].NextAttemptAt.Before(due[j].NextAttemptAt) })
	if len(due) > limit {
		due = due[:limit]
	}

	out := make([]ClaimedDelivery, 0, len(due))
	for i := range due {
		d := f.dels[due[i].ID]
		d.Status = "in_flight"
		d.Attempts++
		t := now
		d.LastAttemptAt = &t
		f.dels[d.ID] = d
		ev := f.events[d.EventID]
		sub := f.subs[d.SubscriptionID]
		out = append(out, ClaimedDelivery{
			ID:           d.ID,
			EventID:      d.EventID,
			EventType:    ev.Type,
			EventPayload: ev.Payload,
			SubURL:       sub.URL,
			SubSecret:    sub.Secret,
			Attempts:     d.Attempts,
		})
	}
	return out, nil
}

// MarkDelivered implements Store.
func (f *Fake) MarkDelivered(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.dels[id]; ok {
		d.Status = "delivered"
		t := f.Now()
		d.DeliveredAt = &t
		f.dels[id] = d
	}
	return nil
}

// MarkDead implements Store.
func (f *Fake) MarkDead(_ context.Context, id string, httpStatus int, errMsg string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.dels[id]; ok {
		d.Status = "dead"
		d.LastHTTPStatus = &httpStatus
		d.LastError = &errMsg
		f.dels[id] = d
	}
	return nil
}

// Reschedule implements Store.
func (f *Fake) Reschedule(_ context.Context, id string, nextAttempt time.Time, httpStatus *int, errMsg *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.dels[id]; ok {
		d.Status = "pending"
		d.NextAttemptAt = nextAttempt
		d.LastHTTPStatus = httpStatus
		d.LastError = errMsg
		f.dels[id] = d
	}
	return nil
}

// RequeueStale implements Store with the same attempt-keyed backoff as SQL.
func (f *Fake) RequeueStale(_ context.Context, olderThan time.Duration) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.Now()
	n := 0
	for id, d := range f.dels {
		if d.Status != "in_flight" || d.LastAttemptAt == nil {
			continue
		}
		if now.Sub(*d.LastAttemptAt) < olderThan {
			continue
		}
		d.Status = "pending"
		d.NextAttemptAt = now.Add(backoffForAttempt(d.Attempts))
		f.dels[id] = d
		n++
	}
	return n, nil
}

// CountDue implements Store.
func (f *Fake) CountDue(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.Now()
	n := 0
	for _, d := range f.dels {
		if d.Status == "pending" && !d.NextAttemptAt.After(now) {
			n++
		}
	}
	return n, nil
}

// Ping implements Store.
func (f *Fake) Ping(_ context.Context) error { return f.PingErr }

func matchesType(types []string, t string) bool {
	if len(types) == 0 {
		return true
	}
	for _, x := range types {
		if x == t {
			return true
		}
	}
	return false
}

// backoffForAttempt mirrors the retry policy: the wait scheduled after the
// n-th claim. attempts is the post-claim value.
func backoffForAttempt(attempts int) time.Duration {
	switch attempts {
	case 1:
		return time.Minute
	case 2:
		return 5 * time.Minute
	case 3:
		return 30 * time.Minute
	default:
		return 2 * time.Hour
	}
}
