# AdTech Homework 1 — Data Normalization

## Overview
This project normalizes a denormalized AdTech dataset into a relational MySQL schema with 5 tables.

## Schema
- `advertisers`: unique advertisers;
- `campaigns`: campaigns linked to advertisers;
- `users`: user profiles;
- `ad_events`: one row per ad impression;
- `clicks`: one row per click, linked to ad_events

## How to run

### 1. Start MySQL
```bash
docker compose up -d
```

### 2. Install Python dependencies
```bash
pip3 install pandas mysql-connector-python
```

### 3. Load data
```bash
python3 scripts/02_load_data.py
```

## Dataset
Three CSV files are required in the `data/` folder:
- `campaigns.csv`
- `users.csv`  
- `ad_events.csv`

## License + Disclaimer
Everything is done for the homework purposes only (Data Engineering Micromasters, SET University, 2026)