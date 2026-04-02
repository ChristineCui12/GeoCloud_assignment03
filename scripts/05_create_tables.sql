-- Part 5: Create BigQuery external tables with hive partitioning
--
-- These tables point to the hive-partitioned folder structure uploaded
-- by 05_upload_to_gcs.py. BigQuery reads the airnow_date partition key
-- from folder names like airnow_date=2024-07-01/, so queries with a
-- WHERE airnow_date = '...' filter only scan that one day's file.
--
-- Replace `geocloud-assignment-03` with your actual GCP project ID if different.


-- ============================================================
-- Hourly Observations — CSV (hive-partitioned)
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.hourly_observations_csv_hive`
WITH PARTITION COLUMNS (
  airnow_date DATE
)
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/csv/*'],
  hive_partition_uri_prefix = 'gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/csv'
);


-- ============================================================
-- Hourly Observations — JSON-L (hive-partitioned)
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.hourly_observations_jsonl_hive`
WITH PARTITION COLUMNS (
  airnow_date DATE
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/jsonl/*'],
  hive_partition_uri_prefix = 'gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/jsonl'
);


-- ============================================================
-- Hourly Observations — Parquet (hive-partitioned)
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.hourly_observations_parquet_hive`
WITH PARTITION COLUMNS (
  airnow_date DATE
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/parquet/*'],
  hive_partition_uri_prefix = 'gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/parquet'
);
