

CREATE TABLE taxi_trips (
  VendorID              INT        NULL,
  tpep_pickup_datetime  TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count       FLOAT      NULL,
  trip_distance         FLOAT      NULL,
  pickup_longitude      FLOAT      NULL,
  pickup_latitude       FLOAT      NULL,
  RateCodeID            FLOAT      NULL,
  store_and_fwd_flag    VARCHAR(2) NULL,
  dropoff_longitude     FLOAT      NULL,
  dropoff_latitude      FLOAT      NULL,
  payment_type          FLOAT      NULL,
  fare_amount           FLOAT      NULL,
  extra                 FLOAT      NULL,
  mta_tax               FLOAT      NULL,
  tip_amount            FLOAT      NULL,
  tolls_amount          FLOAT      NULL,
  improvement_surcharge FLOAT      NULL,
  total_amount          FLOAT      NULL
);

-- Copy data from s3
COPY taxi_trips FROM 's3://milesg-taxi-data-east/yellow'
    IAM_ROLE 'arn:aws:iam::755632011865:role/redshift-role'
    DELIMITER ','
    REGION 'us-east-1'
    TIMEFORMAT AS 'YYYY-MM-DD hh:mi:ss'
    NULL AS ''
    IGNOREHEADER 1
;

-- Create loadview to find any loading errors
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

-- See any errors
SELECT * FROM loadview;

-- Number of records
SELECT COUNT(*) FROM taxi_trips;

-- Passenger counts mean tip groupting
SELECT passenger_count, AVG(tip_amount)
FROM taxi_trips
GROUP BY passenger_count
ORDER BY passenger_count ASC;