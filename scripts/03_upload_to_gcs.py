"""
    Script to upload prepared data files to Google Cloud Storage (GCS).

    This script uploads the transformed files from data/prepared/ to a
    GCS bucket, preserving the folder structure so that BigQuery can
    use wildcard URIs to create external tables across multiple files.

    Prerequisites:
        - Run `gcloud auth application-default login` to authenticate.
        - Create a GCS bucket (manually or in this script).

    Usage:
        python scripts/03_upload_to_gcs.py
"""

import pathlib
from google.cloud import storage


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'
BUCKET_NAME = 'geocloud-assignment-03-christinecui-data'


def upload_prepared_data():
    """Upload all prepared data files to GCS.

    Uploads the contents of data/prepared/ to the GCS bucket,
    preserving the folder structure under a prefix of 'air_quality/'.

    Expected GCS structure:
        gs://<bucket>/air_quality/hourly/2024-07-01.csv
        gs://<bucket>/air_quality/hourly/2024-07-01.jsonl
        gs://<bucket>/air_quality/hourly/2024-07-01.parquet
        ...
        gs://<bucket>/air_quality/sites/site_locations.csv
        gs://<bucket>/air_quality/sites/site_locations.jsonl
        gs://<bucket>/air_quality/sites/site_locations.geoparquet
    """
    client = storage.Client()

    # Create bucket if it doesn't exist
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.exists():
        bucket = client.create_bucket(BUCKET_NAME, location='US')
        print(f'Created bucket {BUCKET_NAME}')
    else:
        print(f'Using existing bucket {BUCKET_NAME}')

    prepared_dir = DATA_DIR / 'prepared'
    uploaded = 0

    for file_path in sorted(prepared_dir.rglob('*')):
        if not file_path.is_file():
            continue
        relative = file_path.relative_to(prepared_dir)
        blob_name = f'air_quality/{relative}'
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(file_path))
        print(f'  Uploaded {blob_name}')
        uploaded += 1

    print(f'Uploaded {uploaded} files to gs://{BUCKET_NAME}/')


if __name__ == '__main__':
    upload_prepared_data()
    print('Done.')
