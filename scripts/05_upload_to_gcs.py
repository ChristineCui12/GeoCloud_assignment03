"""
    Script to re-upload prepared data to GCS using hive-partitioned folder structure.

    This script takes the same prepared files from Part 2 and uploads them
    to GCS with a hive-partitioned directory layout. Instead of flat files like:
        air_quality/hourly/2024-07-01.csv

    Files are organized as:
        air_quality/hourly/csv/airnow_date=2024-07-01/data.csv

    This enables BigQuery to automatically detect the partition key
    (airnow_date) and use it for query pruning, so queries filtering
    by date only scan the relevant files.

    Prerequisites:
        - Run `gcloud auth application-default login` to authenticate.
        - Parts 1-3 should be complete (data already prepared and uploaded once).

    Usage:
        python scripts/05_upload_to_gcs.py
"""

import pathlib
from google.cloud import storage


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'
BUCKET_NAME = 'geocloud-assignment-03-christinecui-data'

# Map file extension to GCS subfolder name
FORMAT_DIRS = {
    '.csv':     'csv',
    '.jsonl':   'jsonl',
    '.parquet': 'parquet',
}


def upload_with_hive_partitioning():
    """Upload prepared hourly data to GCS with hive-partitioned folder structure.

    For each date's prepared files, upload them to GCS with the following
    folder structure:
        gs://<bucket>/air_quality/hourly/csv/airnow_date=2024-07-01/data.csv
        gs://<bucket>/air_quality/hourly/jsonl/airnow_date=2024-07-01/data.jsonl
        gs://<bucket>/air_quality/hourly/parquet/airnow_date=2024-07-01/data.parquet
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    hourly_dir = DATA_DIR / 'prepared' / 'hourly'
    uploaded = 0

    for file_path in sorted(hourly_dir.iterdir()):
        if not file_path.is_file():
            continue
        suffix = file_path.suffix
        if suffix not in FORMAT_DIRS:
            continue

        date_str = file_path.stem          # e.g. '2024-07-01'
        fmt_dir = FORMAT_DIRS[suffix]      # e.g. 'csv'
        data_filename = f'data{suffix}'    # e.g. 'data.csv'

        blob_name = (
            f'air_quality/hourly/{fmt_dir}/'
            f'airnow_date={date_str}/{data_filename}'
        )
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(file_path))
        print(f'  Uploaded {blob_name}')
        uploaded += 1

    print(f'Uploaded {uploaded} files with hive partitioning.')


if __name__ == '__main__':
    upload_with_hive_partitioning()
    print('Done.')
