// Package api wires the HTTP surface of webhook-relay (stdlib ServeMux,
// method patterns). All JSON in/out; errors as {"error":"<code>","message":"..."}.
package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"webhook-relay/internal/ingest"
	"webhook-relay/internal/metrics"
	"webhook-relay/internal/store"
)

// Handler serves the relay's HTTP endpoints.
type Handler struct {
	Store   store.Store
	Log     *slog.Logger
	Metrics *metrics.Metrics // optional; nil in some tests
}

// webhookRequest is the POST /v1/webhooks body.
type webhookRequest struct {
	ID     string          `json:"id"`
	Type   string          `json:"type"`
	Source string          `json:"source"`
	Data   json.RawMessage `json:"data"`
}

// subscriptionRequest is the POST /v1/subscriptions body.
type subscriptionRequest struct {
	URL        string   `json:"url"`
	EventTypes []string `json:"event_types"`
}

// subscriptionView is what GET /v1/subscriptions returns: no secrets.
type subscriptionView struct {
	ID         string    `json:"id"`
	URL        string    `json:"url"`
	EventTypes []string  `json:"event_types"`
	CreatedAt  time.Time `json:"created_at"`
}

// NewRouter builds the application mux (without /metrics, which main wires).
func NewRouter(h *Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/webhooks", h.postWebhook)
	mux.HandleFunc("POST /v1/subscriptions", h.postSubscription)
	mux.HandleFunc("GET /v1/subscriptions", h.listSubscriptions)
	mux.HandleFunc("DELETE /v1/subscriptions/{id}", h.deleteSubscription)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
	return h.withLogging(mux)
}

func (h *Handler) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lrw := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(lrw, r)
		if h.Log != nil {
			h.Log.Info("request",
				slog.String("component", "api"),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", lrw.status),
				slog.Int64("latency_ms", time.Since(start).Milliseconds()),
			)
		}
	})
}

// --- POST /v1/webhooks ---

func (h *Handler) postWebhook(w http.ResponseWriter, r *http.Request) {
	var req webhookRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		h.reject(w, "invalid_json", err)
		return
	}
	if err := ingest.ValidateEventID(req.ID); err != nil {
		h.reject(w, "invalid_event_id", err)
		return
	}
	if err := ingest.ValidateEventType(req.Type); err != nil {
		h.reject(w, "invalid_event_type", err)
		return
	}
	if !isJSONObject(req.Data) {
		h.reject(w, "invalid_data", errors.New("data must be a JSON object"))
		return
	}

	queued, err := h.Store.InsertEventAndDeliveries(r.Context(), store.Event{
		ID:      req.ID,
		Type:    req.Type,
		Source:  sourceOr(req.Source, "unknown"),
		Payload: string(req.Data),
	})
	switch {
	case errors.Is(err, store.ErrEventExists):
		// 409: duplicate producer event. Report when we first saw it.
		ev, gerr := h.Store.GetEvent(r.Context(), req.ID)
		var firstSeen *time.Time
		if gerr == nil {
			firstSeen = &ev.CreatedAt
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":         "event_exists",
			"event_id":      req.ID,
			"first_seen_at": firstSeen,
		})
		return
	case err != nil:
		h.serverError(w, err)
		return
	}

	if h.Metrics != nil {
		h.Metrics.EventsReceived.WithLabelValues(sourceOr(req.Source, "unknown")).Inc()
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"event_id":          req.ID,
		"deliveries_queued": queued,
	})
}

// --- POST /v1/subscriptions ---

func (h *Handler) postSubscription(w http.ResponseWriter, r *http.Request) {
	var req subscriptionRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		h.reject(w, "invalid_json", err)
		return
	}
	if err := ingest.ValidateURL(req.URL); err != nil {
		h.reject(w, "invalid_url", err)
		return
	}
	for _, t := range req.EventTypes {
		if err := ingest.ValidateEventType(t); err != nil {
			h.reject(w, "invalid_event_type", err)
			return
		}
	}

	secret, err := randomSecret()
	if err != nil {
		h.serverError(w, err)
		return
	}

	sub, err := h.Store.CreateSubscription(r.Context(), store.Subscription{
		URL:        req.URL,
		Secret:     secret,
		EventTypes: req.EventTypes,
	})
	if err != nil {
		h.serverError(w, err)
		return
	}
	// The secret is shown exactly once, at creation.
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":     sub.ID,
		"url":    sub.URL,
		"secret": sub.Secret,
	})
}

// --- GET /v1/subscriptions ---

func (h *Handler) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := h.Store.ListSubscriptions(r.Context())
	if err != nil {
		h.serverError(w, err)
		return
	}
	views := make([]subscriptionView, 0, len(subs))
	for _, s := range subs {
		views = append(views, subscriptionView{
			ID:         s.ID,
			URL:        s.URL,
			EventTypes: s.EventTypes,
			CreatedAt:  s.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, views)
}

// --- DELETE /v1/subscriptions/{id} ---

func (h *Handler) deleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := h.Store.DeleteSubscription(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		h.error(w, http.StatusNotFound, "not_found", fmt.Errorf("subscription %s not found", id))
	case err != nil:
		h.serverError(w, err)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// --- /healthz /readyz ---

func (h *Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("ok\n"))
}

func (h *Handler) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.Store.Ping(ctx); err != nil {
		h.error(w, http.StatusServiceUnavailable, "db_unavailable", err)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("ok\n"))
}

// --- helpers ---

// randomSecret returns 32 random bytes, hex-encoded (64 chars).
func randomSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("api: crypto/rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func sourceOr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// isJSONObject reports whether raw is a valid JSON object (or null-safe empty).
func isJSONObject(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	return json.Valid(trimmed)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) reject(w http.ResponseWriter, code string, err error) {
	if h.Metrics != nil {
		h.Metrics.EventsRejected.WithLabelValues(code).Inc()
	}
	h.error(w, http.StatusBadRequest, code, err)
}

func (h *Handler) error(w http.ResponseWriter, code int, codeStr string, err error) {
	writeJSON(w, code, map[string]any{"error": codeStr, "message": err.Error()})
}

func (h *Handler) serverError(w http.ResponseWriter, err error) {
	if h.Log != nil {
		h.Log.Error("request failed", slog.String("component", "api"), slog.Any("error", err))
	}
	h.error(w, http.StatusInternalServerError, "internal", err)
}

// statusRecorder captures the response status for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}
