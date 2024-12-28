CREATE TABLE IF NOT EXISTS {{ params.table }} (
    event_id VARCHAR(255),
    event_type VARCHAR(255),
    event_created_at TIMESTAMP,
    is_public BOOLEAN,
    user_id VARCHAR(255),
    user_login VARCHAR(255),
    user_display_login VARCHAR(255),
    repo_id VARCHAR(255),
    repo_name VARCHAR(255),
    repo_url VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_event_created_at ON {{ params.table }} (event_created_at);
CREATE INDEX IF NOT EXISTS idx_event_type ON {{ params.table }} (event_type);
CREATE INDEX IF NOT EXISTS idx_user_id ON {{ params.table }} (user_id);
