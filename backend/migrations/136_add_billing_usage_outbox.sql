CREATE TABLE IF NOT EXISTS billing_usage_outbox (
    id BIGINT PRIMARY KEY,
    usage_log_id BIGINT NOT NULL UNIQUE,
    user_id BIGINT NOT NULL,
    model TEXT NOT NULL,
    actual_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    usage_created_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS billing_usage_outbox_seq (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    next_seq BIGINT NOT NULL
);

INSERT INTO billing_usage_outbox_seq (id, next_seq)
VALUES (1, 1)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS billing_usage_outbox_gc_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    last_acknowledged_outbox_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO billing_usage_outbox_gc_state (id, last_acknowledged_outbox_id)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_billing_usage_outbox_user_id_id
    ON billing_usage_outbox (user_id, id);

CREATE INDEX IF NOT EXISTS idx_billing_usage_outbox_usage_created_at
    ON billing_usage_outbox (usage_created_at);
