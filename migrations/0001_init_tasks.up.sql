CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB,
    status TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_tasks_status ON tasks(status);