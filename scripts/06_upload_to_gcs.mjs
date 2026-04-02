/**
 * Stretch challenge: Upload merged hourly + site location data to GCS.
 *
 * This script uploads the denormalized (merged) files produced by
 * 06_prepare.mjs to GCS with a hive-partitioned folder structure.
 *
 * Prerequisites:
 *     - Run `gcloud auth application-default login` to authenticate.
 *     - Part 6 prepare script (06_prepare.mjs) should be complete.
 *
 * Usage:
 *     node scripts/06_upload_to_gcs.mjs
 */

import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.resolve(__dirname, '..', 'data');

// TODO: Update this to your bucket name
const BUCKET_NAME = 'geocloud-assignment-03-yourname-data';

/**
 * Upload merged hourly data to GCS with hive-partitioned folder structure.
 *
 * Expected GCS structure:
 *     gs://<bucket>/air_quality/hourly_with_sites/csv/airnow_date=2024-07-01/data.csv
 *     gs://<bucket>/air_quality/hourly_with_sites/jsonl/airnow_date=2024-07-01/data.jsonl
 *     gs://<bucket>/air_quality/hourly_with_sites/geoparquet/airnow_date=2024-07-01/data.geoparquet
 */
async function uploadMergedData() {
  throw new Error('Implement this function to upload merged data to GCS.');
}

await uploadMergedData();
console.log('Done.');
