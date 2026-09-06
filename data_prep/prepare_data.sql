COPY (
    SELECT *
    FROM read_parquet('data/yellow_tripdata_2026-01.parquet')
    LIMIT 100000
)
TO 'data/taxi_100k.parquet'
(FORMAT PARQUET);

COPY (
    SELECT *
    FROM read_parquet('data/yellow_tripdata_2026-01.parquet')
    LIMIT 100000
)
TO 'data/taxi_100k.csv'
(FORMAT CSV, HEADER TRUE);

COPY (
    SELECT *
    FROM read_parquet('data/yellow_tripdata_2026-01.parquet')
    LIMIT 500000
)
TO 'data/taxi_500k.parquet'
(FORMAT PARQUET);

COPY (
    SELECT *
    FROM read_parquet('data/yellow_tripdata_2026-01.parquet')
    LIMIT 500000
)
TO 'data/taxi_500k.csv'
(FORMAT CSV, HEADER TRUE);

COPY (
    SELECT *
    FROM read_parquet('data/yellow_tripdata_2026-01.parquet')
    LIMIT 1000000
)
TO 'data/taxi_1m.parquet'
(FORMAT PARQUET);

COPY (
    SELECT *
    FROM read_parquet('data/yellow_tripdata_2026-01.parquet')
    LIMIT 1000000
)
TO 'data/taxi_1m.csv'
(FORMAT CSV, HEADER TRUE);

COPY (
    SELECT *
    FROM read_parquet('data/yellow_tripdata_2026-01.parquet')
    LIMIT 2000000
)
TO 'data/taxi_2m.parquet'
(FORMAT PARQUET);

COPY (
    SELECT *
    FROM read_parquet('data/yellow_tripdata_2026-01.parquet')
    LIMIT 2000000
)
TO 'data/taxi_2m.csv'
(FORMAT CSV, HEADER TRUE);