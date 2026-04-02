# Assignment 03 Responses

## Part 4: BigQuery External Tables

### Hourly Observations — CSV External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_observations_csv`
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
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly/*.csv']
);
```

### Hourly Observations — JSON-L External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_observations_jsonl`
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
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly/*.jsonl']
);
```

### Hourly Observations — Parquet External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_observations_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly/*.parquet']
);
```

### Site Locations — CSV External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.site_locations_csv`
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/sites/site_locations.csv']
);
```

### Site Locations — JSON-L External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.site_locations_jsonl`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/sites/site_locations.jsonl']
);
```

### Site Locations — GeoParquet External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.site_locations_geoparquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/sites/site_locations.geoparquet']
);
```

### Cross-Table Join Query

```sql
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
```

**Query results** — Average PM2.5 by state on 2024-07-15 (top 50 shown):

| # | State | Avg PM2.5 | Observations |
|---|-------|-----------|--------------|
| 1 | JK | 45.15 | 96 |
| 2 | null | 21.43 | 2070 |
| 3 | OK | 18.70 | 624 |
| 4 | DS | 17.98 | 192 |
| 5 | AR | 17.06 | 142 |
| 6 | AZ | 15.47 | 946 |
| 7 | KS | 14.56 | 382 |
| 8 | SC | 14.52 | 396 |
| 9 | GA | 14.09 | 812 |
| 10 | TX | 14.04 | 2092 |
| 11 | MO | 13.45 | 576 |
| 12 | NC | 13.06 | 1026 |
| 13 | UT | 12.79 | 626 |
| 14 | ID | 12.64 | 1172 |
| 15 | TN | 12.57 | 714 |
| 16 | DC | 12.32 | 138 |
| 17 | NJ | 11.65 | 564 |
| 18 | LA | 11.19 | 522 |
| 19 | KY | 11.13 | 712 |
| 20 | CT | 11.00 | 382 |
| 21 | NY | 10.81 | 1080 |
| 22 | AL | 10.78 | 574 |
| 23 | ME | 10.55 | 430 |
| 24 | VA | 10.54 | 576 |
| 25 | MT | 9.94 | 840 |
| 26 | MA | 9.89 | 564 |
| 27 | MN | 9.78 | 1022 |
| 28 | MD | 9.78 | 442 |
| 29 | PR | 9.70 | 46 |
| 30 | RI | 9.65 | 144 |
| 31 | PA | 9.54 | 1676 |
| 32 | IL | 9.32 | 1092 |
| 33 | IN | 9.24 | 850 |
| 34 | NH | 9.14 | 288 |
| 35 | MS | 8.98 | 340 |
| 36 | VT | 8.97 | 144 |
| 37 | SD | 8.79 | 280 |
| 38 | OH | 8.67 | 1456 |
| 39 | DE | 8.56 | 288 |
| 40 | NM | 8.55 | 526 |
| 41 | IA | 8.55 | 470 |
| 42 | FL | 8.31 | 1830 |
| 43 | WV | 8.06 | 126 |
| 44 | CA | 8.02 | 4896 |
| 45 | NE | 7.88 | 50 |
| 46 | CO | 7.46 | 666 |
| 47 | CC | 7.43 | 5230 |
| 48 | WI | 6.91 | 716 |
| 49 | WY | 6.85 | 220 |
| 50 | OR | 6.81 | 1816 |

---

## Part 5: Hive-Partitioned External Tables

### Hourly Observations — CSV (hive-partitioned)

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_observations_csv_hive`
WITH PARTITION COLUMNS (
  airnow_date DATE
)
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly/csv/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-christinecui-data/air_quality/hourly/csv'
);
```

### Hourly Observations — JSON-L (hive-partitioned)

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_observations_jsonl_hive`
WITH PARTITION COLUMNS (
  airnow_date DATE
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly/jsonl/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-christinecui-data/air_quality/hourly/jsonl'
);
```

### Hourly Observations — Parquet (hive-partitioned)

```sql
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_observations_parquet_hive`
WITH PARTITION COLUMNS (
  airnow_date DATE
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly/parquet/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-christinecui-data/air_quality/hourly/parquet'
);
```

---

## Part 6: Analysis & Reflection

### 1. File Sizes

*Run the scripts and fill in actual sizes with `ls -lh data/prepared/hourly/2024-07-01.*` and `ls -lh data/prepared/sites/`.*

**Hourly data (single day — 2024-07-01):**

| Format  | File Size |
|---------|-----------|
| CSV     | 17 MB     |
| JSON-L  | 42 MB     |
| Parquet | 796 KB    |

**Site locations:**

| Format     | File Size |
|------------|-----------|
| CSV        | 1.0 MB    |
| JSON-L     | 2.8 MB    |
| GeoParquet | 509 KB    |

**Analysis:**
Parquet is by far the smallest because it stores data in a columnar, binary-encoded format with built-in compression (Snappy by default). Each column is stored contiguously, so values of the same type compress extremely well — repeated strings like `parameter_name` or `data_source` collapse to near nothing. CSV is a plain-text row format with no compression and a full copy of every field name implied by position. JSON-L is the largest because it repeats every field key in every row, multiplying the overhead across ~175,000 rows per day (7,300 rows/hour × 24 hours).

### 2. Format Anatomy

**CSV vs. Parquet:**

CSV (Comma/Pipe-Separated Values) is a row-oriented plain-text format. Each row is a newline-terminated string of field values separated by a delimiter. It has no schema embedded in the file (beyond an optional header row), no type information, and no compression. Any tool that can read text can read CSV, which makes it maximally portable — but also maximally verbose. BigQuery must scan every byte of every row to answer even a single-column query.

Parquet is a binary, columnar format. Instead of storing row 1, then row 2, etc., it groups all values of `parameter_name` together, then all values of `value`, and so on. Each column chunk is independently compressed and carries type metadata. This design has two major advantages: (1) a query that only reads `value` and `aqsid` skips the bytes for every other column entirely, and (2) columnar data compresses extremely well because adjacent values are of the same type and often similar magnitude. The trade-off is that Parquet files are binary and not human-readable.

### 3. Choosing Formats for BigQuery

Parquet is preferred over CSV or JSON-L for two reasons:

**Performance:** BigQuery is a columnar query engine. When you run `SELECT AVG(value) FROM ...`, BigQuery only needs to read the `value` column — it can skip all other columns entirely. With CSV or JSON-L, BigQuery must read and parse the entire row to extract a single field, even if the query only touches one column. For a month of AirNow data with ~5 million rows and 9 columns, the difference in bytes scanned is roughly 9×.

**Cost:** BigQuery charges per byte scanned (on-demand pricing). Because Parquet is both columnar (fewer columns scanned per query) and compressed (fewer bytes per column), it can reduce query costs by an order of magnitude compared to CSV for the same logical data. JSON-L is the worst on both dimensions: it's row-oriented like CSV but larger than CSV due to repeated key names.

### 4. Pipeline vs. Warehouse Joins

**Separate tables, join at query time (this assignment's approach):**
- Pros: The site locations file is stored once (~1 MB), not duplicated into every day's hourly file. Schema changes to site metadata require updating only one file. If the join logic changes (e.g., using `FullAQSID` instead of `AQSID`), you re-query without re-processing the raw data. The pipeline stays simple: extract → transform each source independently → load.
- Cons: Every query that needs geographic context must perform a join, which adds latency and scanned bytes. BigQuery handles joins well, but for very high-frequency dashboard queries this overhead accumulates.

**Denormalized (joined at prepare time, Part 6 approach):**
- Pros: Queries are simpler — no join needed, just filter and aggregate. Slightly faster for read-heavy dashboards because the engine never has to look up site metadata. The GeoParquet output is ready for direct spatial analysis in tools like GeoPandas without an extra join step.
- Cons: The site metadata (latitude, longitude, state, etc.) is duplicated across ~175,000 rows per day × 31 days = ~5.4 million rows. Any correction to site metadata requires re-running the entire prepare + upload pipeline. The GeoParquet files are substantially larger.

**When to prefer each:** For a production dashboard serving many users, the denormalized approach reduces per-query cost and complexity — worth the storage overhead. For exploratory analysis or a pipeline where the schema is still evolving, keeping tables separate is more maintainable and flexible.

#### Stretch Challenge (Part 6)

```sql
-- Merged Hourly + Sites — CSV (hive-partitioned)
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_with_sites_csv`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'CSV',
  skip_leading_rows = 1,
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly_with_sites/csv/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-christinecui-data/air_quality/hourly_with_sites/csv'
);
```

```sql
-- Merged Hourly + Sites — JSON-L (hive-partitioned)
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_with_sites_jsonl`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly_with_sites/jsonl/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-christinecui-data/air_quality/hourly_with_sites/jsonl'
);
```

```sql
-- Merged Hourly + Sites — GeoParquet (hive-partitioned)
CREATE OR REPLACE EXTERNAL TABLE `musa5090-s26.air_quality.hourly_with_sites_geoparquet`
WITH PARTITION COLUMNS (airnow_date DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://musa5090-s26-christinecui-data/air_quality/hourly_with_sites/geoparquet/*'],
  hive_partition_uri_prefix = 'gs://musa5090-s26-christinecui-data/air_quality/hourly_with_sites/geoparquet'
);
```

### 5. Choosing a Data Source

**a) A parent who wants a dashboard showing current air quality near their child's school:**
> **AirNow API.** This use case requires near-real-time data refreshed hourly, and the queries are small and targeted — one location, current conditions. The AirNow API is designed exactly for this: it returns current AQI for a specific location or bounding box, without requiring the caller to download and process multi-megabyte flat files. Building a pipeline around the bulk files would add unnecessary infrastructure for what is essentially a "give me the latest reading near this point" query. The AirNow API's rate limits are not a concern here because the dashboard only queries once per user session.

**b) An environmental justice advocate identifying neighborhoods with chronically poor air quality over the past decade:**
> **AQS bulk downloads.** This use case requires quality-assured historical data spanning 10+ years — exactly what AQS publishes. AirNow data is near-real-time and not quality-controlled, making it unreliable for trend analysis over long periods. The AQS bulk download files (annual CSVs by pollutant and geography) are the right format: they're large enough to justify building a local pipeline (rather than hammering the rate-limited AQS API with decade-spanning queries), and they've undergone the QA/QC review needed to support policy arguments.

**c) A school administrator who needs automated morning alerts when AQI exceeds a threshold:**
> **AirNow API.** This is an operational, recurring, time-sensitive decision — every morning before school. The AirNow API returns current AQI instantly for a given location. A simple scheduled script (cron job or Cloud Scheduler) can call the API each morning, compare the AQI to the threshold, and send an alert email if exceeded. Building a full file-download pipeline would be over-engineered for a single-location threshold check. The API call is small, fast, and purpose-built for this pattern.
