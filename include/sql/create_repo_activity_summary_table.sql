CREATE TABLE IF NOT EXISTS repo_activity_summary (
    aggregation_date DATE,
    repo_id VARCHAR(255),
    repo_name VARCHAR(255),
    total_events INT
);
