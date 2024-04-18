-- Create Schema for Raw Layer if not exists
CREATE SCHEMA IF NOT EXISTS "raw_layer";

-- Create Airbnb Listing Table
CREATE TABLE IF NOT EXISTS "raw_layer".airbnb_listing (
    listing_id INT PRIMARY KEY,
    scrape_id INT,
    scrape_date DATE,
    host_id INT,
    host_name VARCHAR(255),
    host_since DATE,
    host_is_superhost VARCHAR(255),
    host_neighborhood VARCHAR(255),
    listing_neighbourhood VARCHAR(255),
    property_type VARCHAR(255),
    room_type VARCHAR(255),
    accommodates INT,
    price DECIMAL(10, 2),
    has_availability VARCHAR(255),
    availability_30 INT,
    number_of_reviews INT,
    review_scores_rating INT,
    review_scores_accuracy INT,
    review_scores_cleanliness INT,
    review_scores_checkin INT,
    review_scores_communication INT,
    review_scores_value INT
);

-- Create G02 Table
CREATE TABLE IF NOT EXISTS "raw_layer".g02 (
    LGA_CODE_2016 VARCHAR(255) PRIMARY KEY,
    median_age_persons INT,
    median_mortgage_repay_monthly INT,
    median_tot_prsnl_inc_weekly INT,
    median_rent_weekly INT,
    median_tot_fami_inc_weekly INT,
    average_num_psns_per_bedroom INT,
    median_tot_hhd_inc_weekly INT,
    average_household_size INT
);

-- Create NSW LGA Table
CREATE TABLE IF NOT EXISTS "raw_layer".nsw_lga (
    lga_code INT PRIMARY KEY,
    lga_name VARCHAR(255),
    suburb_name VARCHAR(255)
);

-- Create Snapshot Tables for G01
CREATE TABLE IF NOT EXISTS "Raw".g01_snapshot (
    LGA_CODE_2016 VARCHAR(255),
    median_age_persons INT,
    median_mortgage_repay_monthly INT,
    median_tot_prsnl_inc_weekly INT,
    median_rent_weekly INT,
    median_tot_fami_inc_weekly INT,
    average_num_psns_per_bedroom INT,
    median_tot_hhd_inc_weekly INT,
    average_household_size INT,
    snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (LGA_CODE_2016, snapshot_timestamp)
);

-- Create Snapshot Tables for G02
CREATE TABLE IF NOT EXISTS "Raw".g02_snapshot (
    LGA_CODE_2016 VARCHAR(255),
    median_age_persons INT,
    median_mortgage_repay_monthly INT,
    median_tot_prsnl_inc_weekly INT,
    median_rent_weekly INT,
    median_tot_fami_inc_weekly INT,
    average_num_psns_per_bedroom INT,
    median_tot_hhd_inc_weekly INT,
    average_household_size INT,
    snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (LGA_CODE_2016, snapshot_timestamp)
);

