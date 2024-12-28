CREATE TABLE IF NOT EXISTS daily_event_summary (
    aggregation_date DATE,
    event_type VARCHAR(255),
    total_events INT,
    total_public_events INT,
    total_private_events INT,
    PRIMARY KEY (aggregation_date, event_type)
);
