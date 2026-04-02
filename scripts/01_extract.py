"""
    Script to extract AirNow data files for a range of dates.

    This script downloads hourly air quality observation data and monitoring
    site location data from the EPA's AirNow program. Files are saved into
    a date-organized folder structure under data/raw/.

    AirNow files are hosted at:
        https://files.airnowtech.org/?prefix=airnow/

    Usage:
        python scripts/01_extract.py
"""

import pathlib
import urllib.request
import datetime


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'
BASE_URL = 'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow'


def download_data_for_date(date_str):
    """Download AirNow data files for a single date.

    Downloads all 24 HourlyData files (hours 00-23) and the
    Monitoring_Site_Locations_V2.dat file for the specified date,
    saving them into data/raw/YYYY-MM-DD/.

    Args:
        date_str: Date string in 'YYYY-MM-DD' format. For example, '2024-07-01'.
    """
    date = datetime.date.fromisoformat(date_str)
    year = date.strftime('%Y')
    date_compact = date.strftime('%Y%m%d')

    out_dir = DATA_DIR / 'raw' / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    # Download 24 hourly observation files (hours 00-23)
    for hour in range(24):
        filename = f'HourlyData_{date_compact}{hour:02d}.dat'
        url = f'{BASE_URL}/{year}/{date_compact}/{filename}'
        dest = out_dir / filename
        if not dest.exists():
            print(f'  Downloading {filename}...')
            urllib.request.urlretrieve(url, dest)
        else:
            print(f'  Already exists, skipping {filename}')

    # Download monitoring site locations
    sites_filename = 'Monitoring_Site_Locations_V2.dat'
    sites_url = f'{BASE_URL}/{year}/{date_compact}/{sites_filename}'
    sites_dest = out_dir / sites_filename
    if not sites_dest.exists():
        print(f'  Downloading {sites_filename}...')
        urllib.request.urlretrieve(sites_url, sites_dest)
    else:
        print(f'  Already exists, skipping {sites_filename}')


if __name__ == '__main__':
    # Download data for July 2024
    start_date = datetime.date(2024, 7, 1)
    end_date = datetime.date(2024, 7, 31)

    current_date = start_date
    while current_date <= end_date:
        print(f'Downloading data for {current_date}...')
        download_data_for_date(current_date.isoformat())
        current_date += datetime.timedelta(days=1)

    print('Done.')
