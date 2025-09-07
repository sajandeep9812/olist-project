# Olist Project Starter

This repo contains a minimal template for working with the Olist dataset and MySQL.

## Setup
1. Create python virtualenv
   `python3 -m venv .venv && source .venv/bin/activate`
2. Install deps
   `pip install -r requirements.txt`
3. Set DB credentials in environment variables (recommended):
   `export OLIST_DB_USER=root`
   `export OLIST_DB_PASS=yourpass`
   `export OLIST_DB_HOST=localhost`
   `export OLIST_DB_NAME=olist`

## Usage
- Run ETL: `python scripts/etl.py`
- Open notebook: `jupyter lab` and open `notebooks/eda.ipynb`