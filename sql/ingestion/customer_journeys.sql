-- Builds customer journeys using conversions AND session_sources
WITH conversion_timestamps AS (
    SELECT 
        conv_id,
        user_id,
        conv_date || ' ' || conv_time AS conversion_datetime,
        CASE 
            WHEN revenue > 0 THEN 1 
            ELSE 0 
        END AS conversion
    FROM conversions

),
session_timestamps AS (
    SELECT 
        *,
        event_date || ' ' || event_time AS session_datetime
    FROM session_sources
)
SELECT 
    c.conv_id AS conversion_id,
    s.session_id,
    s.channel_name AS channel_label,
    s.impression_interaction,
    s.holder_engagement,
    s.closer_engagement,
    c.conversion,
    s.session_datetime,
    c.conversion_datetime AS "timestamp"
FROM conversion_timestamps c
JOIN session_timestamps s 
    ON s.user_id = c.user_id
    AND s.session_datetime < c.conversion_datetime
WHERE 1=1
    AND (:start_date IS NULL OR DATE(c.conversion_datetime) >= DATE(:start_date))
    AND (:end_date IS NULL OR DATE(c.conversion_datetime) <= DATE(:end_date))
    AND (:start_date IS NULL OR DATE(s.session_datetime) >= DATE(:start_date))
    AND (:end_date IS NULL OR DATE(s.session_datetime) <= DATE(:end_date))
ORDER BY 
    c.conv_id,
    session_datetime;
