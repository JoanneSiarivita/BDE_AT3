-- Create Schema for Warehouse Layer
CREATE SCHEMA if not exists "warehouse_layer";

-- Dimension table for Airbnb Host
CREATE TABLE dim_airbnb_host (
    host_id INT PRIMARY KEY,
    host_name VARCHAR(255),
    host_since DATE,
    host_is_superhost VARCHAR(255),
    host_neighborhood VARCHAR(255)
);

-- Dimension table for Airbnb Listing
CREATE TABLE dim_airbnb_listing (
    listing_id INT PRIMARY KEY,
    property_type VARCHAR(255),
    room_type VARCHAR(255),
    accomodates INT,
    price DECIMAL(10, 2),
    has_availability VARCHAR(255),
    scrape_date DATE,
    listing_neighbourhood VARCHAR(255)
);

-- Dimension table for NSW LGA
CREATE TABLE dim_nsw_lga (
    lga_code INT PRIMARY KEY,
    lga_name VARCHAR(255),
    suburb_name VARCHAR(255)
);

-- Fact table for Airbnb Reviews
CREATE TABLE facts_airbnb_reviews (
    review_id SERIAL PRIMARY KEY,
    listing_id INT REFERENCES dim_airbnb_listing(listing_id),
    host_id INT REFERENCES dim_airbnb_host(host_id),
    lga_code INT REFERENCES dim_nsw_lga(lga_code),
    scrape_id INT,
    number_of_reviews INT,
    review_scores_rating INT,
    review_scores_accuracy INT,
    review_scores_cleanliness INT,
    review_scores_checkin INT,
    review_scores_communication INT,
    review_scores_value INT,
    review_date DATE
);
