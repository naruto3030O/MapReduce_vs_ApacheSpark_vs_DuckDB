-- AI-ASSISTED: A7
-- Task 5 - DuckDB: repeated work on the same data.
--
-- The same aggregation as Task 1, run five times against the same Parquet
-- file in a single DuckDB connection.
--
-- {{INPUT}} is substituted with the input file path by duckdb/run_task5.py.

SELECT
    PULocationID,
    COUNT(*)          AS trip_count,
    AVG(fare_amount)  AS avg_fare
FROM read_parquet('{{INPUT}}')
WHERE PULocationID IS NOT NULL
  AND fare_amount  IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID;
