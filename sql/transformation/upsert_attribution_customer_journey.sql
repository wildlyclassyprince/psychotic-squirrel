INSERT OR REPLACE INTO attribution_customer_journey(
    conv_id,
    session_id,
    ihc
)
SELECT
    conversion_id as conv_id,
    session_id,
    ihc
FROM temp_attribution_customer_journey;
