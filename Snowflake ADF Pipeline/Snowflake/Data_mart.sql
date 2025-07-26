CREATE SCHEMA IF NOT EXISTS comp_price_proj.data_mart;

CREATE TABLE IF NOT EXISTS comp_price_proj.data_mart.fact_price_tracking (
    title STRING,
    brand STRING,
    price FLOAT,
    reviews INT,
    ratings FLOAT,
    source STRING,
    category STRING
    
);