"""
    Stretch challenge: Prepare merged hourly + site location data.

    This script joins the hourly observation data with site location data
    during the prepare step (denormalization), producing files where
    each observation row includes the site's latitude, longitude, and
    other geographic metadata.

    Usage:
        python scripts/06_prepare.py
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

# Site location columns to carry into the merged output
SITE_KEEP_COLS = [
    'AQSID',
    'Latitude',
    'Longitude',
    'StateAbbreviation',
    'CountyName',
    'MSAName',
]


def _read_hourly_for_date(date_str):
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
            df = df.drop_duplicates(subset='AQSID', keep='first')
            available = [c for c in SITE_KEEP_COLS if c in df.columns]
            return df[available]
    raise FileNotFoundError('No Monitoring_Site_Locations_V2.dat found in data/raw/')


def _merge(date_str):
    hourly = _read_hourly_for_date(date_str)
    sites = _read_site_locations()
    merged = hourly.merge(sites, left_on='aqsid', right_on='AQSID', how='left')
    merged = merged.drop(columns=['AQSID'], errors='ignore')
    return merged


def prepare_merged_csv(date_str):
    """Merge hourly observations with site locations and write as CSV."""
    out_dir = DATA_DIR / 'prepared' / 'hourly_with_sites'
    out_dir.mkdir(parents=True, exist_ok=True)
    _merge(date_str).to_csv(out_dir / f'{date_str}.csv', index=False)


def prepare_merged_jsonl(date_str):
    """Merge hourly observations with site locations and write as JSON-L."""
    out_dir = DATA_DIR / 'prepared' / 'hourly_with_sites'
    out_dir.mkdir(parents=True, exist_ok=True)
    _merge(date_str).to_json(out_dir / f'{date_str}.jsonl', orient='records', lines=True)


def prepare_merged_geoparquet(date_str):
    """Merge hourly observations with site locations and write as GeoParquet."""
    out_dir = DATA_DIR / 'prepared' / 'hourly_with_sites'
    out_dir.mkdir(parents=True, exist_ok=True)
    df = _merge(date_str)
    geometry = [
        Point(lon, lat) if pd.notna(lon) and pd.notna(lat) else None
        for lon, lat in zip(df['Longitude'], df['Latitude'])
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    gdf.to_parquet(out_dir / f'{date_str}.geoparquet')


if __name__ == '__main__':
    import datetime

    start_date = datetime.date(2024, 7, 1)
    end_date = datetime.date(2024, 7, 31)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.isoformat()
        print(f'Preparing merged data for {date_str}...')
        prepare_merged_csv(date_str)
        prepare_merged_jsonl(date_str)
        prepare_merged_geoparquet(date_str)
        current_date += datetime.timedelta(days=1)

    print('Done.')
