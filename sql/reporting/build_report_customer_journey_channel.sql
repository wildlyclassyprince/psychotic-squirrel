WITH attributed_sessions AS (
    -- Combine our attribution data with session data
    SELECT 
        ss.session_id,
        ss.channel_name,
        ss.event_date as "date",
        acj.ihc,
        acj.conv_id as conversion_id,
        c.revenue,
        sc.cost
    FROM session_sources ss
    LEFT JOIN attribution_customer_journey acj 
        ON ss.session_id = acj.session_id
    LEFT JOIN session_costs sc 
    	ON sc.session_id = ss.session_id
    LEFT JOIN conversions c
        ON acj.conv_id = c.conv_id
),
daily_channel_metrics AS (
    -- Aggregate everything at the channel-date level
    -- We need to ensure uniqueness and non-nullity!
    SELECT 
        channel_name,
        "date",
        COUNT(DISTINCT session_id) as visits,
        COUNT(DISTINCT conversion_id) as conversions,
        COALESCE(SUM(revenue * ihc), 0) as ihc_revenue,
        COALESCE(SUM(ihc), 0) as ihc,
        COALESCE(SUM(cost), 0) as cost
    FROM attributed_sessions
    GROUP BY 
        channel_name,
        "date"
    -- Don't include under and over attributed values in the report
    HAVING ABS(SUM(ihc) - 1.0) between 0.0 and 1.0
)
INSERT OR REPLACE INTO channel_reporting(
    channel_name,
    "date",
    cost,
    ihc,
    ihc_revenue
)
SELECT 
    d.channel_name,
    d."date",
    d.cost,
    round(d.ihc, 2) as ihc,
    round(d.ihc_revenue, 2) as ihc_revenue
FROM daily_channel_metrics d;
