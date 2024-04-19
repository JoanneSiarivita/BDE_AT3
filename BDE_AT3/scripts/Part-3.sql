--Q1

-- Calculate estimated revenue per listing for each neighborhood
WITH RevenuePerNeighborhood AS (
    SELECT 
        LISTING_NEIGHBOURHOOD,
        AVG(PRICE * AVAILABILITY_30 * 12) AS estimated_revenue_per_listing
    FROM 
        airbnb_data
    WHERE 
        SCRAPED_DATE BETWEEN '2020-05-01' AND '2021-04-30'
    GROUP BY 
        LISTING_NEIGHBOURHOOD
),

-- Identify the best and worst performing neighborhoods
BestPerformingNeighborhood AS (
    SELECT 
        LISTING_NEIGHBOURHOOD
    FROM 
        RevenuePerNeighborhood
    ORDER BY 
        estimated_revenue_per_listing DESC
    LIMIT 1
),

WorstPerformingNeighborhood AS (
    SELECT 
        LISTING_NEIGHBOURHOOD
    FROM 
        RevenuePerNeighborhood
    ORDER BY 
        estimated_revenue_per_listing ASC
    LIMIT 1
),

-- Calculate the population difference between the best and worst performing neighborhoods
PopulationDifference AS (
    SELECT 
        ABS(neigh1.Population - neigh2.Population) AS population_difference
    FROM 
        (SELECT SUM(Tot_P_P) AS Population FROM census_data WHERE LGA_NAME = (SELECT * FROM BestPerformingNeighborhood)) AS neigh1,
        (SELECT SUM(Tot_P_P) AS Population FROM census_data WHERE LGA_NAME = (SELECT * FROM WorstPerformingNeighborhood)) AS neigh2
)

-- Output the population difference
SELECT 
    population_difference
FROM 
    PopulationDifference;

   -- Q2
   WITH RevenuePerListingType AS (
    SELECT 
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        COUNT(*) AS num_stays,
        AVG(PRICE * AVAILABILITY_30 * 12) AS estimated_revenue_per_listing
    FROM 
        airbnb_data
    WHERE 
        SCRAPED_DATE BETWEEN '2020-05-01' AND '2021-04-30'
        AND HAS_AVAILABILITY = 't' -- Consider only active listings
    GROUP BY 
        PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES
),

BestListingType AS (
    SELECT 
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES
    FROM 
        RevenuePerListingType
    WHERE 
        estimated_revenue_per_listing = (
            SELECT MAX(estimated_revenue_per_listing) 
            FROM RevenuePerListingType
        )
    ORDER BY 
        num_stays DESC
    LIMIT 1
)

SELECT 
    PROPERTY_TYPE,
    ROOM_TYPE,
    ACCOMMODATES
FROM 
    BestListingType;

 -- Q3 
   WITH HostListingsCount AS (
    SELECT 
        HOST_ID,
        COUNT(DISTINCT LISTING_ID) AS num_listings
    FROM 
        airbnb_data
    GROUP BY 
        HOST_ID
    HAVING 
        COUNT(DISTINCT LISTING_ID) > 1 -- Consider only hosts with multiple listings
),

HostListingsInSameLGA AS (
    SELECT 
        HOST_ID
    FROM 
        airbnb_data AS ad
    JOIN 
        HostListingsCount AS hlc ON ad.HOST_ID = hlc.HOST_ID
    GROUP BY 
        HOST_ID
    HAVING 
        COUNT(DISTINCT LGA_CODE) = 1 -- Consider only hosts with listings in a single LGA
),

TotalHostsWithMultipleListings AS (
    SELECT 
        COUNT(*) AS total_hosts
    FROM 
        HostListingsCount
),

HostsWithListingsInSameLGA AS (
    SELECT 
        COUNT(*) AS hosts_in_same_lga
    FROM 
        HostListingsInSameLGA
)

SELECT 
    hosts_in_same_lga,
    total_hosts,
    CASE
        WHEN total_hosts > 0 THEN (hosts_in_same_lga * 100.0 / total_hosts)
        ELSE 0
    END AS percentage_hosts_in_same_lga
FROM 
    HostsWithListingsInSameLGA, TotalHostsWithMultipleListings;

--Q4
WITH UniqueHostListings AS (
    SELECT 
        HOST_ID,
        LISTING_ID,
        AVG(PRICE * AVAILABILITY_30 * 12) AS estimated_revenue_per_listing
    FROM 
        airbnb_data
    WHERE 
        SCRAPED_DATE BETWEEN '2020-05-01' AND '2021-04-30'
    GROUP BY 
        HOST_ID, LISTING_ID
    HAVING 
        COUNT(*) = 1 -- Consider only hosts with a unique listing
),

HostsWithUniqueListings AS (
    SELECT 
        HOST_ID,
        SUM(estimated_revenue_per_listing) AS total_estimated_revenue
    FROM 
        UniqueHostListings
    GROUP BY 
        HOST_ID
),

HostsWithMortgageComparison AS (
    SELECT 
        h.HOST_ID,
        h.total_estimated_revenue,
        c.Median_mortgage_repay_monthly * 12 AS annualized_median_mortgage_repayment
    FROM 
        HostsWithUniqueListings AS h
    JOIN 
        airbnb_data AS ad ON h.HOST_ID = ad.HOST_ID
    JOIN 
        census_data AS c ON ad.LGA_CODE_2016 = c.LGA_CODE_2016
)

SELECT 
    HOST_ID,
    total_estimated_revenue,
    annualized_median_mortgage_repayment,
    CASE
        WHEN total_estimated_revenue > annualized_median_mortgage_repayment THEN 'Revenue Higher'
        ELSE 'Mortgage Higher'
    END AS comparison_result
FROM 
    HostsWithMortgageComparison;
