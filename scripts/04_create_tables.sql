-- Part 4: Create BigQuery external tables
--
-- Run these statements in the BigQuery console.
-- Replace `geocloud-assignment-03` with your actual GCP project ID if different.
-- Dataset: air_quality
--
-- After creating the tables, verify row counts with:
--     SELECT count(*) FROM `geocloud-assignment-03.air_quality.<table_name>`;


-- ============================================================
-- Hourly Observations — CSV
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.hourly_observations_csv`
(
  valid_date       STRING,
  valid_time       STRING,
  aqsid            STRING,
  site_name        STRING,
  gmt_offset       FLOAT64,
  parameter_name   STRING,
  reporting_units  STRING,
  value            FLOAT64,
  data_source      STRING
)
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/*.csv']
);


-- ============================================================
-- Hourly Observations — JSON-L
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.hourly_observations_jsonl`
(
  valid_date       STRING,
  valid_time       STRING,
  aqsid            STRING,
  site_name        STRING,
  gmt_offset       FLOAT64,
  parameter_name   STRING,
  reporting_units  STRING,
  value            FLOAT64,
  data_source      STRING
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/*.jsonl']
);


-- ============================================================
-- Hourly Observations — Parquet
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.hourly_observations_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/hourly/*.parquet']
);


-- ============================================================
-- Site Locations — CSV
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.site_locations_csv`
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/sites/site_locations.csv']
);


-- ============================================================
-- Site Locations — JSON-L
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.site_locations_jsonl`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/sites/site_locations.jsonl']
);


-- ============================================================
-- Site Locations — GeoParquet
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `geocloud-assignment-03.air_quality.site_locations_geoparquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://geocloud-assignment-03-christinecui-data/air_quality/sites/site_locations.geoparquet']
);


-- ============================================================
-- Verify row counts (run each separately after table creation)
-- ============================================================
-- SELECT count(*) FROM `geocloud-assignment-03.air_quality.hourly_observations_csv`;
-- SELECT count(*) FROM `geocloud-assignment-03.air_quality.hourly_observations_jsonl`;
-- SELECT count(*) FROM `geocloud-assignment-03.air_quality.hourly_observations_parquet`;


-- ============================================================
-- Cross-table join: average PM2.5 by state for 2024-07-15
-- ============================================================
SELECT
  s.StateAbbreviation                        AS state,
  ROUND(AVG(h.value), 2)                     AS avg_pm25,
  COUNT(*)                                   AS observation_count
FROM
  `geocloud-assignment-03.air_quality.hourly_observations_parquet` AS h
INNER JOIN
  `geocloud-assignment-03.air_quality.site_locations_geoparquet`   AS s
  ON h.aqsid = s.AQSID
WHERE
  h.parameter_name = 'PM2.5'
  AND h.valid_date  = '07/15/24'
GROUP BY
  s.StateAbbreviation
ORDER BY
  avg_pm25 DESC;
