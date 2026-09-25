package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"webhook-relay/internal/api"
	"webhook-relay/internal/store"
)

func newTestServer(t *testing.T, st store.Store) *httptest.Server {
	t.Helper()
	h := &api.Handler{Store: st}
	ts := httptest.NewServer(api.NewRouter(h))
	t.Cleanup(ts.Close)
	return ts
}

// doJSON performs one request and returns the response plus its body decoded
// as a JSON object (nil body → nil map).
func doJSON(t *testing.T, method, url, body string) (*http.Response, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, url, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	var out map[string]any
	if respBody, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read response: %v", err)
	} else if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &out); err != nil {
			t.Fatalf("decode response %q: %v", respBody, err)
		}
	}
	return resp, out
}

// --- fan-out counting (0 subs, all subs, filtered subs) ---

func TestPostWebhookFanOut(t *testing.T) {
	cases := []struct {
		name  string
		event string
		subs  []store.Subscription
		want  int
	}{
		{"no subs", `{"id":"e1","type":"payment.paid","data":{}}`, nil, 0},
		{"all-type sub", `{"id":"e2","type":"payment.paid","data":{}}`,
			[]store.Subscription{{URL: "https://a.example/h", EventTypes: []string{}}}, 1},
		{"filtered match", `{"id":"e3","type":"payment.paid","data":{}}`,
			[]store.Subscription{{URL: "https://b.example/h", EventTypes: []string{"payment.paid", "refund.issued"}}, {URL: "https://c.example/h", EventTypes: []string{"refund.issued"}}}, 1},
		{"filtered none", `{"id":"e4","type":"payment.paid","data":{}}`,
			[]store.Subscription{{URL: "https://d.example/h", EventTypes: []string{"refund.issued"}}}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := store.NewFake()
			for i := range tc.subs {
				if _, err := fake.CreateSubscription(context.Background(), tc.subs[i]); err != nil {
					t.Fatalf("create sub: %v", err)
				}
			}
			ts := newTestServer(t, fake)
			resp, body := doJSON(t, http.MethodPost, ts.URL+"/v1/webhooks", tc.event)
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 (body %v)", resp.StatusCode, body)
			}
			if got := body["deliveries_queued"].(float64); int(got) != tc.want {
				t.Errorf("deliveries_queued = %v, want %d", body["deliveries_queued"], tc.want)
			}
		})
	}
}

// --- 409 duplicate event ---

func TestPostWebhookDuplicate409(t *testing.T) {
	fake := store.NewFake()
	ts := newTestServer(t, fake)

	body := `{"id":"dup-1","type":"payment.paid","data":{"k":"v"}}`
	resp, _ := doJSON(t, http.MethodPost, ts.URL+"/v1/webhooks", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("first post status = %d, want 202", resp.StatusCode)
	}

	resp, got := doJSON(t, http.MethodPost, ts.URL+"/v1/webhooks", body)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("dup post status = %d, want 409", resp.StatusCode)
	}
	if got["error"] != "event_exists" {
		t.Errorf("error = %v, want event_exists", got["error"])
	}
	if got["event_id"] != "dup-1" {
		t.Errorf("event_id = %v, want dup-1", got["event_id"])
	}
	if got["first_seen_at"] == nil {
		t.Error("first_seen_at missing from 409 body")
	}
}

// --- validation: every 400 path ---

func TestPostWebhookValidation400(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantCode string
	}{
		{"bad json", `{`, "invalid_json"},
		{"missing id", `{"type":"payment.paid","data":{}}`, "invalid_event_id"},
		{"bad id chars", `{"id":"BAD ID","type":"payment.paid","data":{}}`, "invalid_event_id"},
		{"missing type", `{"id":"ok-id","data":{}}`, "invalid_event_type"},
		{"bad type", `{"id":"ok-id","type":"single","data":{}}`, "invalid_event_type"},
		{"missing data", `{"id":"ok-id","type":"payment.paid"}`, "invalid_data"},
		{"data is array", `{"id":"ok-id","type":"payment.paid","data":[1,2]}`, "invalid_data"},
		{"data invalid json", `{"id":"ok-id","type":"payment.paid","data":{"k":`, "invalid_json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestServer(t, store.NewFake())
			resp, got := doJSON(t, http.MethodPost, ts.URL+"/v1/webhooks", tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			if got["error"] != tc.wantCode {
				t.Errorf("error = %v, want %q", got["error"], tc.wantCode)
			}
		})
	}
}

// --- subscriptions ---

func TestPostSubscription(t *testing.T) {
	fake := store.NewFake()
	ts := newTestServer(t, fake)

	resp, got := doJSON(t, http.MethodPost, ts.URL+"/v1/subscriptions",
		`{"url":"https://x.example/hooks","event_types":["payment.paid"]}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	secret, _ := got["secret"].(string)
	if len(secret) != 64 {
		t.Errorf("secret length = %d, want 64 hex chars", len(secret))
	}
	if !isHex(secret) {
		t.Errorf("secret %q is not hex", secret)
	}
	if got["url"] != "https://x.example/hooks" {
		t.Errorf("url = %v", got["url"])
	}
}

func TestPostSubscriptionValidation400(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantCode string
	}{
		{"bad url scheme", `{"url":"ftp://x.example"}`, "invalid_url"},
		{"missing host", `{"url":"https://"}`, "invalid_url"},
		{"bad event type", `{"url":"https://x.example","event_types":["NOPE"]}`, "invalid_event_type"},
		{"bad json", `{"url":`, "invalid_json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestServer(t, store.NewFake())
			resp, got := doJSON(t, http.MethodPost, ts.URL+"/v1/subscriptions", tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			if got["error"] != tc.wantCode {
				t.Errorf("error = %v, want %q", got["error"], tc.wantCode)
			}
		})
	}
}

func TestListSubscriptionsNoSecrets(t *testing.T) {
	const canary = "secret-should-not-appear"
	fake := store.NewFake()
	if _, err := fake.CreateSubscription(context.Background(), store.Subscription{
		URL: "https://x.example/hooks", Secret: canary,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	ts := newTestServer(t, fake)

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/subscriptions", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte(canary)) {
		t.Errorf("list response leaked secret: %s", body)
	}
}

func TestDeleteSubscription(t *testing.T) {
	fake := store.NewFake()
	created, err := fake.CreateSubscription(context.Background(), store.Subscription{URL: "https://x.example/h"})
	if err != nil {
		t.Fatal(err)
	}
	ts := newTestServer(t, fake)

	resp, _ := doJSON(t, http.MethodDelete, ts.URL+"/v1/subscriptions/"+created.ID, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", resp.StatusCode)
	}
	resp, _ = doJSON(t, http.MethodDelete, ts.URL+"/v1/subscriptions/"+created.ID, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("repeat delete status = %d, want 404", resp.StatusCode)
	}
}

// --- health / readiness ---

func TestHealthz(t *testing.T) {
	ts := newTestServer(t, store.NewFake())
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz status = %d, want 200", resp.StatusCode)
	}
}

func TestReadyzFailingStore(t *testing.T) {
	fake := store.NewFake()
	fake.PingErr = errors.New("db down")
	ts := newTestServer(t, fake)

	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("readyz status = %d, want 503", resp.StatusCode)
	}
}

func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return len(s) > 0
}
