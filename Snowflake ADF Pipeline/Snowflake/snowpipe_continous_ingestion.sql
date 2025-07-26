USE ROLE ACCOUNTADMIN;
USE WAREHOUSE compute_wh;
USE DATABASE COMP_PRICE_PROJ;
USE SCHEMA my_schema;

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

  CREATE OR REPLACE TRANSIENT TABLE raw_data (
    title VARCHAR,
    brand VARCHAR,
    price VARCHAR,
    reviews VARCHAR,
    ratings VARCHAR,
    source VARCHAR,
    category VARCHAR
);

CREATE STORAGE INTEGRATION snowpipe_az_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = AZURE
ENABLED = TRUE
AZURE_TENANT_ID = '7fda2d2b-86e5-4eb2-9888-db4c95094912'
STORAGE_ALLOWED_LOCATIONS = ('azure://scrapeddata21.blob.core.windows.net/raw');

DESC STORAGE INTEGRATION snowpipe_az_int;


create stage az_stage
STORAGE_INTEGRATION = snowpipe_az_int
URL = 'azure://scrapeddata21.blob.core.windows.net/raw'
FILE_FORMAT = my_csv_format;

LIST @az_stage

CREATE NOTIFICATION INTEGRATION snowpipe_event
ENABLED = true
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://scrapeddata21.queue.core.windows.net/snowpipequeue'
AZURE_TENANT_ID = '7fda2d2b-86e5-4eb2-9888-db4c95094912'

DESC notification integration snowpipe_event;






CREATE OR REPLACE PIPE amazon_pipe
AUTO_INGEST = TRUE
INTEGRATION = 'SNOWPIPE_EVENT'
AS
COPY INTO RAW_DATA
FROM (
  SELECT
    $1::VARCHAR AS title,
    $2::VARCHAR AS brand,
    $3::VARCHAR AS price,
    $4::VARCHAR AS rating,
    $5::VARCHAR AS reviews,
    'amazon' AS source,
    REGEXP_SUBSTR(METADATA$FILENAME, 'dbo_raw_amazon_(.+?)\.csv', 1, 1, 'e') AS category
  FROM @az_stage/amazon
)
FILE_FORMAT = (
  TYPE = 'CSV',
  SKIP_HEADER = 1,
  FIELD_OPTIONALLY_ENCLOSED_BY = '"',
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'continue';

ALTER PIPE amazon_pipe REFRESH;


CREATE OR REPLACE PIPE flipkart_pipe
AUTO_INGEST = TRUE
INTEGRATION = 'SNOWPIPE_EVENT'
AS
COPY INTO RAW_DATA
FROM (
  SELECT
    $1::VARCHAR AS title,
    $2::VARCHAR AS brand,
    $3::VARCHAR AS price,
    $4::VARCHAR AS rating,
    $5::VARCHAR AS reviews,
    'flipkart' AS source,
    REGEXP_SUBSTR(METADATA$FILENAME, 'dbo_raw_flipkart_(.+?)\.csv', 1, 1, 'e') AS category
  FROM @az_stage/flipkart
)
FILE_FORMAT = (
  TYPE = 'CSV',
  SKIP_HEADER = 1,
  FIELD_OPTIONALLY_ENCLOSED_BY = '"',
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'continue';

ALTER PIPE flipkart_pipe REFRESH;
