CREATE TABLE taxi_trips (
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  tip_amount FLOAT,
  fare_amount FLOAT,
  total_amount FLOAT,
  vendor_id VARCHAR(5),
  passenger_count FLOAT,
  trip_distance FLOAT,
  payment_type VARCHAR(5),
  tolls_amount FLOAT
);


COPY taxi_trips from 's3://milesg-taxi-data-east/*.csv' IAM_ROLE 'arn:aws:iam::755632011865:role/redshift-role'