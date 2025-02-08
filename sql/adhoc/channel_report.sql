SELECT
    cr.*,
    case 
        when round(cr.cost / cr.ihc, 2) is null then 0
        else round(cr.cost / cr.ihc, 2)
    end as cpo,
    case
        when round(cr.ihc_revenue / cr.cost, 2) is null then 0
        else round(cr.ihc_revenue / cr.cost, 2)
    end as roas
FROM channel_reporting cr;
