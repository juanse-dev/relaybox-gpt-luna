CREATE TABLE deliveries (
    id TEXT PRIMARY KEY NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    target_url TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL
);
