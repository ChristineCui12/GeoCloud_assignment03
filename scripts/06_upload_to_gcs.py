"""
    Stretch challenge: Upload merged hourly + site location data to GCS.

    This script uploads the denormalized (merged) files produced by
    06_prepare.py to GCS with a hive-partitioned folder structure.

    Prerequisites:
        - Run `gcloud auth application-default login` to authenticate.
        - Part 6 prepare script (06_prepare.py) should be complete.

    Usage:
        python scripts/06_upload_to_gcs.py
"""

import pathlib
from google.cloud import storage


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'
BUCKET_NAME = 'geocloud-assignment-03-christinecui-data'

FORMAT_DIRS = {
    '.csv':         'csv',
    '.jsonl':       'jsonl',
    '.geoparquet':  'geoparquet',
}


def upload_merged_data():
    """Upload merged hourly data to GCS with hive-partitioned folder structure.

    Expected GCS structure:
        gs://<bucket>/air_quality/hourly_with_sites/csv/airnow_date=2024-07-01/data.csv
        gs://<bucket>/air_quality/hourly_with_sites/jsonl/airnow_date=2024-07-01/data.jsonl
        gs://<bucket>/air_quality/hourly_with_sites/geoparquet/airnow_date=2024-07-01/data.geoparquet
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    merged_dir = DATA_DIR / 'prepared' / 'hourly_with_sites'
    uploaded = 0

    for file_path in sorted(merged_dir.iterdir()):
        if not file_path.is_file():
            continue
        suffix = file_path.suffix
        if suffix not in FORMAT_DIRS:
            continue

        date_str = file_path.stem
        fmt_dir = FORMAT_DIRS[suffix]
        data_filename = f'data{suffix}'

        blob_name = (
            f'air_quality/hourly_with_sites/{fmt_dir}/'
            f'airnow_date={date_str}/{data_filename}'
        )
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(file_path))
        print(f'  Uploaded {blob_name}')
        uploaded += 1

    print(f'Uploaded {uploaded} merged files with hive partitioning.')


if __name__ == '__main__':
    upload_merged_data()
    print('Done.')
