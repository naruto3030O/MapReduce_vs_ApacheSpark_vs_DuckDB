-- AI-ASSISTED: A4
-- Task 2 - DuckDB: Parquet filtering and projection.
--
-- Fixed filter: PULocationID = 161 AND fare_amount > 30.00
--   Query A returns all columns for matching rows.
--   Query B returns only four columns for the same rows.
--
-- {{INPUT}} is substituted with the input file path by duckdb/run_task2.py.
-- To run this file directly in the DuckDB CLI, replace {{INPUT}} with the
-- path, e.g. 'data/taxi_2m.parquet'.
--
-- The two queries are separated by a split marker line, which run_task2.py
-- uses to load them individually.

-- Query A: all columns
SELECT *
FROM read_parquet('{{INPUT}}')
WHERE PULocationID = 161
  AND fare_amount > 30.00;

--@SPLIT

-- Query B: projection to four columns
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount
FROM read_parquet('{{INPUT}}')
WHERE PULocationID = 161
  AND fare_amount > 30.00;
