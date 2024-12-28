CREATE TABLE IF NOT EXISTS user_activity_summary (
    aggregation_date DATE,
    user_id VARCHAR(255),
    total_events INT,
    total_public_events INT,
    total_private_events INT,
    PRIMARY KEY (aggregation_date, user_id)
);
