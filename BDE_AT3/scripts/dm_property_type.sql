-- Create Schema for Datamart Layer
CREATE schema if not exists datamart_layer;

CREATE VIEW dm_property_type AS
SELECT
    l.property_type,
    l.room_type,
    l.accommodates,
    l.month_year,
    COUNT(DISTINCT l.listing_id) AS total_listings,
    SUM(CASE WHEN l.status = 'active' THEN 1 ELSE 0 END) AS active_listings,
    SUM(CASE WHEN l.status = 'inactive' THEN 1 ELSE 0 END) AS inactive_listings,
    AVG(l.price) AS avg_price_active_listings,
    MIN(l.price) AS min_price_active_listings,
    MAX(l.price) AS max_price_active_listings,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.price) OVER (PARTITION BY l.property_type, l.room_type, l.accommodates, l.month_year) AS median_price_active_listings,
    COUNT(DISTINCT l.host_id) AS distinct_hosts,
    SUM(CASE WHEN l.superhost = 'yes' THEN 1 ELSE 0 END) AS superhost_count,
    AVG(l.review_scores_rating) AS avg_review_scores_rating,
    (active_listings - LAG(active_listings) OVER (PARTITION BY l.property_type, l.room_type, l.accommodates ORDER BY l.month_year)) / NULLIF(LAG(active_listings) OVER (PARTITION BY l.property_type, l.room_type, l.accommodates ORDER BY l.month_year), 0) * 100 AS active_listings_percentage_change,
    (inactive_listings - LAG(inactive_listings) OVER (PARTITION BY l.property_type, l.room_type, l.accommodates ORDER BY l.month_year)) / NULLIF(LAG(inactive_listings) OVER (PARTITION BY l.property_type, l.room_type, l.accommodates ORDER BY l.month_year), 0) * 100 AS inactive_listings_percentage_change,
    SUM(s.number_of_stays) AS total_number_of_stays,
    AVG(s.estimated_revenue) AS avg_estimated_revenue_per_active_listing
FROM
    dim_airbnb_listing
LEFT JOIN
   dim_airbnb_host ON l.listing_id = s.listing_id
GROUP BY
    l.property_type,
    l.room_type,
    l.accommodates,
    l.month_year
ORDER BY
    l.property_type,
    l.room_type,
    l.accommodates,
    l.month_year;
