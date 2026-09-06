-- AI-ASSISTED: A5
-- Task 3 - DuckDB: join with the taxi-zone lookup.
--
-- Join PULocationID with LocationID, keep rows with fare_amount >= 0,
-- total fare_amount per Borough + Zone, return the five pickup zones with
-- the highest total fare revenue. Ties broken by Zone name ascending.
--
-- The two placeholder tokens are substituted by duckdb/run_task3.py with the
-- taxi Parquet path and the zone lookup CSV path.
--
-- "Zone" is quoted because it is a reserved word in DuckDB.

SELECT
    z.Borough,
    z."Zone",
    SUM(t.fare_amount) AS total_fare
FROM read_parquet('{{INPUT}}') AS t
JOIN read_csv('{{LOOKUP}}', HEADER = TRUE) AS z
    ON t.PULocationID = z.LocationID
WHERE t.fare_amount >= 0
GROUP BY z.Borough, z."Zone"
ORDER BY total_fare DESC, z."Zone" ASC
LIMIT 5;
