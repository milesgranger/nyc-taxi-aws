DROP TABLE IF EXISTS taxi_trips;

-- Define the taxi_trips table
CREATE TABLE taxi_trips (
  pickup_datetime TIMESTAMP NULL DISTKEY,
  dropoff_datetime TIMESTAMP NULL,
  tip_amount FLOAT NULL,
  fare_amount FLOAT NULL,
  total_amount FLOAT NULL,
  vendor_id VARCHAR(155) NULL,
  passenger_count FLOAT NULL,
  trip_distance FLOAT NULL,
  payment_type VARCHAR(155) NULL,
  tolls_amount FLOAT NULL
);

-- Create loadview table to look up errors on loading events
CREATE VIEW loadview AS
  (SELECT DISTINCT
     tbl,
     trim(name)       AS taxi_trips,
     query,
     starttime,
     trim(filename)   AS input,
     line_number,
     colname,
     err_code,
     trim(err_reason) AS reason
   FROM stl_load_errors sl, stv_tbl_perm sp
   WHERE sl.tbl = sp.id);

-- Load data from S3
COPY taxi_trips FROM 's3://milesg-taxi-data-east/yellow'
    IAM_ROLE 'arn:aws:iam::755632011865:role/redshift-role'
    DELIMITER ','
    GZIP
    TIMEFORMAT AS 'YYYY-MM-DD hh:mi:ss'
    NULL AS ''
    IGNOREHEADER 1;

SELECT * FROM loadview;

-- Summarized table for QuickSight
drop table IF EXISTS grouped_amounts;
CREATE TABLE grouped_amounts AS (
  SELECT
    avg(tip_amount) as avg_tip_amount,
    avg(fare_amount) as avg_fare_amount,
    avg(tolls_amount) as avg_tolls_amount,
    avg(trip_distance) as avg_trip_distance,
    count(*) as group_count,
    extract(HOUR FROM pickup_datetime) AS hour_pickup,
    extract(YEAR FROM pickup_datetime) AS year_pickup,
    extract(MONTH FROM pickup_datetime) AS month_pickup
  FROM taxi_trips
  GROUP BY year_pickup, month_pickup, hour_pickup
);

-- check the number of records.
SELECT count(*) FROM taxi_trips;