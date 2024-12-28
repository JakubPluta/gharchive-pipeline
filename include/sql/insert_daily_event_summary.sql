INSERT INTO daily_event_summary (aggregation_date, event_type, total_events, total_public_events, total_private_events)
SELECT
    DATE(event_created_at) AS aggregation_date,
    event_type,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE is_public) AS total_public_events,
    COUNT(*) FILTER (WHERE NOT is_public) AS total_private_events
FROM {{ params.table }}
WHERE event_created_at >= '{{ prev_execution_date  }}'
  AND event_created_at < '{{ execution_date }}'
GROUP BY aggregation_date, event_type
ON CONFLICT (aggregation_date, event_type) DO UPDATE
SET
    total_events = EXCLUDED.total_events,
    total_public_events = EXCLUDED.total_public_events,
    total_private_events = EXCLUDED.total_private_events;
