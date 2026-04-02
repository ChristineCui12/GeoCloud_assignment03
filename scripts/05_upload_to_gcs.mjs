/**
 * Script to re-upload prepared data to GCS using hive-partitioned folder structure.
 *
 * This script takes the same prepared files from Part 2 and uploads them
 * to GCS with a hive-partitioned directory layout. Instead of flat files like:
 *     air_quality/hourly/2024-07-01.csv
 *
 * Files are organized as:
 *     air_quality/hourly/csv/airnow_date=2024-07-01/data.csv
 *
 * This enables BigQuery to automatically detect the partition key
 * (airnow_date) and use it for query pruning, so queries filtering
 * by date only scan the relevant files.
 *
 * This is a backfill of the upload step — you don't need to re-download
 * or re-transform anything. You're just re-uploading the same files
 * with a different folder structure.
 *
 * Prerequisites:
 *     - Run `gcloud auth application-default login` to authenticate.
 *     - Parts 1-3 should be complete (data already prepared and uploaded once).
 *
 * Usage:
 *     node scripts/05_upload_to_gcs.mjs
 */

import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.resolve(__dirname, '..', 'data');

// TODO: Update this to your bucket name
const BUCKET_NAME = 'geocloud-assignment-03-yourname-data';

/**
 * Upload prepared hourly data to GCS with hive-partitioned folder structure.
 *
 * For each date's prepared files, upload them to GCS with the following
 * folder structure:
 *     gs://<bucket>/air_quality/hourly/csv/airnow_date=2024-07-01/data.csv
 *     gs://<bucket>/air_quality/hourly/jsonl/airnow_date=2024-07-01/data.jsonl
 *     gs://<bucket>/air_quality/hourly/parquet/airnow_date=2024-07-01/data.parquet
 *
 * The site locations files don't need hive partitioning (they're not
 * date-partitioned), so you can re-upload them as-is or skip them.
 */
async function uploadWithHivePartitioning() {
  throw new Error('Implement this function to upload with hive partitioning.');
}

await uploadWithHivePartitioning();
console.log('Done.');
