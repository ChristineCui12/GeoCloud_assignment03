"""
    Script to transform raw AirNow data files into BigQuery-compatible formats.

    This script reads the raw .dat files downloaded by 01_extract.py and converts
    them into CSV, JSON-L, and Parquet formats suitable for loading into
    BigQuery as external tables.

    Hourly observation data is converted to: CSV, JSON-L, Parquet
    Site location data is converted to: CSV, JSON-L, GeoParquet (with point geometry)

    Usage:
        python scripts/02_prepare.py
"""

import pathlib
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'

HOURLY_COLUMNS = [
    'valid_date',
    'valid_time',
    'aqsid',
    'site_name',
    'gmt_offset',
    'parameter_name',
    'reporting_units',
    'value',
    'data_source',
]


def _read_hourly_for_date(date_str):
    """Read and concatenate all 24 hourly .dat files for a given date."""
    raw_dir = DATA_DIR / 'raw' / date_str
    frames = []
    for hour in range(24):
        date_compact = date_str.replace('-', '')
        filepath = raw_dir / f'HourlyData_{date_compact}{hour:02d}.dat'
        if filepath.exists():
            df = pd.read_csv(
                filepath,
                sep='|',
                header=None,
                names=HOURLY_COLUMNS,
                encoding='latin-1',
                low_memory=False,
            )
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=HOURLY_COLUMNS)


def _read_site_locations():
    """Read the most recent site locations file and deduplicate by AQSID."""
    raw_dirs = sorted((DATA_DIR / 'raw').iterdir(), reverse=True)
    for raw_dir in raw_dirs:
        filepath = raw_dir / 'Monitoring_Site_Locations_V2.dat'
        if filepath.exists():
            df = pd.read_csv(
                filepath,
                sep='|',
                header=0,
                encoding='latin-1',
                low_memory=False,
            )
            # One row per site (deduplicate by AQSID)
            df = df.drop_duplicates(subset='AQSID', keep='first')
            return df
    raise FileNotFoundError('No Monitoring_Site_Locations_V2.dat found in data/raw/')


# --- Hourly observation data ---

def prepare_hourly_csv(date_str):
    """Convert raw hourly .dat files for a date to a single CSV file."""
    out_dir = DATA_DIR / 'prepared' / 'hourly'
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _read_hourly_for_date(date_str)
    df.to_csv(out_dir / f'{date_str}.csv', index=False)


def prepare_hourly_jsonl(date_str):
    """Convert raw hourly .dat files for a date to newline-delimited JSON."""
    out_dir = DATA_DIR / 'prepared' / 'hourly'
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _read_hourly_for_date(date_str)
    df.to_json(out_dir / f'{date_str}.jsonl', orient='records', lines=True)


def prepare_hourly_parquet(date_str):
    """Convert raw hourly .dat files for a date to Parquet format."""
    out_dir = DATA_DIR / 'prepared' / 'hourly'
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _read_hourly_for_date(date_str)
    df.to_parquet(out_dir / f'{date_str}.parquet', index=False)


# --- Site location data ---

def prepare_site_locations_csv():
    """Convert monitoring site locations to CSV."""
    out_dir = DATA_DIR / 'prepared' / 'sites'
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _read_site_locations()
    df.to_csv(out_dir / 'site_locations.csv', index=False)


def prepare_site_locations_jsonl():
    """Convert monitoring site locations to newline-delimited JSON."""
    out_dir = DATA_DIR / 'prepared' / 'sites'
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _read_site_locations()
    df.to_json(out_dir / 'site_locations.jsonl', orient='records', lines=True)


def prepare_site_locations_geoparquet():
    """Convert monitoring site locations to GeoParquet with point geometry."""
    out_dir = DATA_DIR / 'prepared' / 'sites'
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _read_site_locations()
    geometry = [Point(lon, lat) for lon, lat in zip(df['Longitude'], df['Latitude'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    gdf.to_parquet(out_dir / 'site_locations.geoparquet')


if __name__ == '__main__':
    import datetime

    # Prepare site locations (only need to do this once)
    print('Preparing site locations...')
    prepare_site_locations_csv()
    prepare_site_locations_jsonl()
    prepare_site_locations_geoparquet()

    # Prepare hourly data for each day in July 2024 (backfill)
    start_date = datetime.date(2024, 7, 1)
    end_date = datetime.date(2024, 7, 31)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.isoformat()
        print(f'Preparing hourly data for {date_str}...')
        prepare_hourly_csv(date_str)
        prepare_hourly_jsonl(date_str)
        prepare_hourly_parquet(date_str)
        current_date += datetime.timedelta(days=1)

    print('Done.')
