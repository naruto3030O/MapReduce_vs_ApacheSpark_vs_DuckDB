-- Task 1 - DuckDB SQL implementation.
--
-- For every non-null PULocationID, count trips and average fare_amount,
-- considering only rows where fare_amount is non-null and fare_amount >= 0.
--
-- {{INPUT}} is substituted with the input file path by duckdb/run_task1.py.
-- To run this file directly in the DuckDB CLI, replace {{INPUT}} with the
-- path, e.g. 'data/taxi_1m.csv'.

SELECT
    PULocationID,
    COUNT(*)          AS trip_count,
    AVG(fare_amount)  AS avg_fare
FROM read_csv('{{INPUT}}', HEADER = TRUE)
WHERE PULocationID IS NOT NULL
  AND fare_amount  IS NOT NULL
  AND fare_amount >= 0
GROUP BY PULocationID;
