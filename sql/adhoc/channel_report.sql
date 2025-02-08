SELECT
    cr.*,
    COALESCE(ROUND(cr.cost / cr.ihc, 2), 0) as cpo,
    COALESCE(ROUND(cr.ihc_revenue / cr.cost, 2), 0) as roas
FROM channel_reporting cr;
