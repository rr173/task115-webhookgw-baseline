package store

import "database/sql"

// schema is the SQLite DDL for the webhook delivery gateway. It is applied
// idempotently on open. All timestamps are stored as Unix milliseconds.
const schema = `
CREATE TABLE IF NOT EXISTS subscriptions (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    endpoint        TEXT NOT NULL,
    events          TEXT NOT NULL,
    signing_secret  TEXT NOT NULL,
    rate_limit      INTEGER NOT NULL,
    max_attempts    INTEGER NOT NULL,
    base_delay_ms   INTEGER NOT NULL,
    max_delay_ms    INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id         TEXT PRIMARY KEY,
    type       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS attempts (
    id              TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempt_count   INTEGER NOT NULL,
    last_error      TEXT NOT NULL,
    next_attempt_at INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letters (
    attempt_id       TEXT PRIMARY KEY,
    subscription_id  TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    payload          TEXT NOT NULL,
    reason           TEXT NOT NULL,
    attempt_count    INTEGER NOT NULL,
    created_at       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attempts_subscription ON attempts(subscription_id);
CREATE INDEX IF NOT EXISTS idx_attempts_status ON attempts(status);
CREATE INDEX IF NOT EXISTS idx_attempts_next ON attempts(next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
`

// migrate applies the schema.
func migrate(db *sql.DB) error {
	_, err := db.Exec(schema)
	return err
}
