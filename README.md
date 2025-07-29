# WeatherUnion Data Receiver

A Python utility to fetch meteorological parameters from the Weather Union API for a list of localities and save them as CSV.

## Features

- Rotates through multiple API keys to respect usage quotas
- Configurable list of localities and API keys
- Outputs timestamped CSVs in a date‑based folder
- Simple, extensible module structure

## Installation

```bash
git clone https://github.com/your‑username/weatherunion-data-receiver.git
cd weatherunion-data-receiver
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
