-- Create Schema for Datamart Layer
CREATE schema if not exists datamart_layer;

CREATE VIEW dm_host_neighbourhood AS
SELECT
    CASE 
        WHEN l.host_neighbourhood = 'Bondi' THEN 'Waverley'
        -- Add other transformation cases as needed
        ELSE l.host_neighbourhood 
    END AS host_neighbourhood_lga,
    l.month_year,
    COUNT(DISTINCT l.host_id) AS distinct_hosts,
    SUM(s.estimated_revenue) AS estimated_revenue,
    SUM(s.estimated_revenue) / COUNT(DISTINCT l.host_id) AS estimated_revenue_per_host
FROM
    dim_airbnb_listing
LEFT JOIN
    dim_airbnb_host ON l.listing_id = s.listing_id
GROUP BY
    host_neighbourhood_lga,
    l.month_year
ORDER BY
    host_neighbourhood_lga,
    l.month_year;
