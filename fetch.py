#!/usr/bin/env python3
"""
Fetch weather data for a list of localities from Weather Union API,
rotating through multiple API keys and saving results as CSV.
"""

import os
import csv
import time
import logging
from datetime import datetime
from configparser import ConfigParser

import pandas as pd
import requests

# ─── Configuration ─────────────────────────────────────────────────────────────
config = ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config.ini'))

LOCATIONS_CSV    = config['paths']['locations_csv']
API_KEYS_CSV     = config['paths']['api_keys_csv']
OUTPUT_BASE_DIR  = config['paths']['output_base_dir']

BASE_URL         = config['api']['base_url']
MAX_CALLS_PER_KEY = int(config['api'].get('max_calls_per_key', 1000))

# ─── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def load_csv(path):
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        logging.error(f"CSV file not found: {path}")
        raise

def get_weather_data(station_id, api_key):
    headers = {
        'x-zomato-api-key': api_key,
        'Content-Type': 'application/json'
    }
    params = {'locality_id': station_id}

    try:
        resp = requests.get(BASE_URL, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            logging.warning("Quota limit reached for key %s", api_key)
            return 'QUOTA_LIMIT_REACHED'
        logging.error("HTTP %s fetching station %s", resp.status_code, station_id)
    except requests.RequestException as e:
        logging.error("Request error for station %s: %s", station_id, e)
    return None

def fetch_and_save():
    locations = load_csv(LOCATIONS_CSV)
    api_keys  = load_csv(API_KEYS_CSV)['API_KEY'].tolist()

    results = []
    key_idx, call_count = 0, 0

    start = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    for _, row in locations.iterrows():
        station_id = row['localityId']

        # Try current key until quota exhausted
        while key_idx < len(api_keys):
            key = api_keys[key_idx]
            data = get_weather_data(station_id, key)

            if data == 'QUOTA_LIMIT_REACHED':
                key_idx += 1
                call_count = 0
                continue

            # parse weather_data fields if present
            if data and 'locality_weather_data' in data:
                w = data['locality_weather_data']
                record = {
                    'Station ID': station_id,
                    'Locality Name': row.get('localityName', ''),
                    'Latitude': row.get('latitude', ''),
                    'Longitude': row.get('longitude', ''),
                    'Observation Datetime': datetime.now().isoformat(),
                    'Temperature': w.get('temperature'),
                    'Humidity': w.get('humidity'),
                    'Wind Speed': w.get('wind_speed'),
                    'Wind Direction': w.get('wind_direction'),
                    'Rain Intensity': w.get('rain_intensity'),
                    'Total Rainfall': w.get('rain_accumulation'),
                }
                results.append(record)
                call_count += 1
            else:
                logging.info("No data for station %s", station_id)

            # advance key if needed
            if call_count >= MAX_CALLS_PER_KEY:
                key_idx += 1
                call_count = 0
            break

        if key_idx >= len(api_keys):
            logging.error("All API keys exhausted; stopping early.")
            break

    # write CSV
    date_dir = datetime.now().strftime("%Y%m%d")
    out_dir = os.path.join(OUTPUT_BASE_DIR, date_dir)
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"weather_data_{timestamp}.csv")

    with open(out_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()) if results else [])
        writer.writeheader()
        writer.writerows(results)

    duration = time.time() - start
    logging.info("Wrote %d records to %s (%.2fs)", len(results), out_file, duration)

if __name__ == "__main__":
    fetch_and_save()
