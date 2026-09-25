CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE events (
    id          TEXT PRIMARY KEY,                 -- producer-chosen, unique (UUID v4 recommended)
    type        TEXT NOT NULL,                    -- e.g. 'payment.paid'
    source      TEXT NOT NULL DEFAULT 'unknown',
    payload     JSONB NOT NULL,                   -- the producer's data
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE subscriptions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url         TEXT NOT NULL,                    -- must be http(s)://; NOT unique — multiple subs to one endpoint are allowed by design
    secret      TEXT NOT NULL,                    -- 32 random bytes, hex-encoded; shown once
    event_types TEXT[] NOT NULL DEFAULT '{}',     -- empty array = receive all types
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id         TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    subscription_id  UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    status           TEXT NOT NULL DEFAULT 'pending',   -- pending | in_flight | delivered | dead
    attempts         INT  NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at  TIMESTAMPTZ,
    last_http_status INT,
    last_error       TEXT,
    delivered_at     TIMESTAMPTZ
);

-- one delivery row per (event, subscription) pair — the idempotency anchor
CREATE UNIQUE INDEX uniq_deliveries_pair ON deliveries (event_id, subscription_id);

-- worker claim index
CREATE INDEX idx_deliveries_claim ON deliveries (next_attempt_at)
    WHERE status = 'pending';
