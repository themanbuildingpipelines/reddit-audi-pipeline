--SQL DDL Script for Silver 'Base' Layer

--Drop and recreate table if it exists
IF OBJECT_ID('Silver.audi_data_base', 'U') IS NOT NULL
    DROP TABLE Silver.audi_data_base;

CREATE TABLE Silver.audi_data_base (
    id NVARCHAR(255) PRIMARY KEY,       -- post ID from Reddit
    author NVARCHAR(255),
    num_of_comments INT,
    url NVARCHAR(500),

    user_issue NVARCHAR(MAX),           -- merged title + selftext
    issue_raise_date DATETIME,          -- converted from created_utc
    issue_category NVARCHAR(500),       -- flattened categories (comma-separated)

    car_model NVARCHAR(100),            -- extracted Audi model
    year_model INT,                     -- extracted year
    mileage INT,                        -- extracted mileage (normalized to number)

    dwh_create_date DATETIME DEFAULT GETDATE()
);

