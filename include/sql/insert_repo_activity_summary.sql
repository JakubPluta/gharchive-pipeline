INSERT INTO repo_activity_summary (aggregation_date, repo_id, repo_name, total_events)
SELECT
    DATE(event_created_at) AS aggregation_date,
    repo_id,
    repo_name,
    COUNT(*) AS total_events
FROM {{ params.table }}
WHERE event_created_at >= '{{ prev_execution_date  }}'
  AND event_created_at < '{{ execution_date }}'
GROUP BY aggregation_date, repo_id, repo_name
ON CONFLICT (aggregation_date, repo_id, repo_name) DO UPDATE
SET total_events = EXCLUDED.total_events;

