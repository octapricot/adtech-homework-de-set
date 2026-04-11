# AdTech Data Engineering Homeworks

## Overview
A complete (1-5) data engineering micromasters' course project built on top of an AdTech dataset containing 10 million ad events, 700,000 users, and 1,013 campaigns.

## Dataset
Three CSV files are required in the `data/` folder (not included in repo):
- `campaigns.csv`: 1,013 campaigns with budget and targeting info
- `users.csv`: 700,000 user profiles with demographics and interests
- `ad_events.csv`: 10,000,000 ad impression and click events

## Project Structure

```
adtech-homework/
├── .env.example
├── .gitignore
├── README.md
├── docker-compose.yml
├── data/                          # CSVs — not in repo
│   ├── campaigns.csv
│   ├── users.csv
│   └── ad_events.csv
├── sql/
│   ├── 01_create_tables.sql       # HW1: MySQL schema
│   ├── 02_create_indexes.sql      # HW2: MySQL indexes
│   ├── 03_analysis_queries.sql    # HW2: MySQL analytics queries
│   ├── 04_mongo_queries.js        # HW3: MongoDB queries
│   ├── 05_cassandra_schema.cql    # HW4: Cassandra schema
│   └── 06_cassandra_queries.cql   # HW4: Cassandra queries
├── scripts/
│   ├── 02_load_data.py            # HW1: MySQL loader
│   ├── 03_generate_report.py      # HW2: MySQL report
│   ├── 04_load_mongo.py           # HW3: MongoDB loader
│   ├── 05_mongo_report.py         # HW3: MongoDB report
│   ├── 06_load_cassandra.py       # HW4: Cassandra loader
│   ├── 07_cassandra_queries.py    # HW4: Cassandra report
│   ├── 08_api.py                  # HW5: FastAPI REST API
│   └── 09_benchmark.py            # HW5: Benchmarking script
└── reports/                       
    ├── adtech_report.json
    ├── mongo_report.json
    ├── cassandra_report.json
    └── benchmark_results.json
```

---

## Homework 1: Data Normalization and preparation

The task was to to analyze the provided denormalized dataset, identify redundancy and anomalies, and design a relational schema that improves data integrity and query efficiency.

### Schema (5 tables)
- `advertisers`: unique advertisers, no duplication
- `campaigns`: campaign metadata linked to advertisers
- `users`: user profiles
- `ad_events`: one row per impression
- `clicks`: one row per click, linked to ad_events via event_id

### Files
| File | Purpose |
|---|---|
| `sql/01_create_tables.sql` | DDL: creates all 5 MySQL tables |
| `scripts/02_load_data.py` | Reads CSVs and loads into MySQL in batches |
| `docker-compose.yml` | Spins up MySQL locally |

### How to run
```bash
# Start MySQL
docker compose up -d mysql

# Install dependencies
pip3 install pandas mysql-connector-python python-dotenv

# Load data
python3 scripts/02_load_data.py
```

---

## Homework 2: SQL Complex Querying and Performance

The task was to work with the normalized database schema from Homework 1 and write SQL queries to analyze ad campaign performance.

### Files
| File | Purpose |
|---|---|
| `sql/02_create_indexes.sql` | Creates 7 indexes on ad_events for performance |
| `sql/03_analysis_queries.sql` | All 7 analytical SQL queries |
| `scripts/03_generate_report.py` | Runs queries and exports JSON report |

### How to run
```bash
# Create indexes (run once after MySQL is loaded)
docker exec -i adtech_mysql mysql -u root -p$DB_PASSWORD adtech < sql/02_create_indexes.sql

# Run queries and generate report
python3 scripts/03_generate_report.py
```

---

## Homework 3: User Engagement Tracking with MongoDB

The task was to extend the relational database from Homework 1 by introducing NoSQL storage for user engagement data using MongoDB. 

### Schema
One collection: `user_engagement`
Each document = one user with embedded sessions and impressions.

```json
{
  "user_id": 42,
  "age": 28,
  "interests": ["Technology", "Sports"],
  "sessions": [
    {
      "session_id": "42_Mobile_2024-10-01",
      "device": "Mobile",
      "impressions": [
        {
          "campaign_name": "Campaign_191",
          "was_clicked": true,
          "click": {
            "click_timestamp": "2024-10-01T14:23:05",
            "ad_revenue": 4.41
          }
        }
      ]
    }
  ]
}
```

### Files
| File | Purpose |
|---|---|
| `sql/04_mongo_queries.js` | All 5 MongoDB queries |
| `scripts/04_load_mongo.py` | Builds user documents from CSVs and loads into MongoDB |
| `scripts/05_mongo_report.py` | Runs queries and exports JSON report |

### How to run
```bash
# Start MongoDB
docker compose up -d mongodb

# Install dependencies
pip3 install pymongo

# Load data (takes ~30 minutes for 10M events)
python3 scripts/04_load_mongo.py

# Run queries and generate report
python3 scripts/05_mongo_report.py
```

---

## Homework 4: Ad Performance Analytics with Cassandra

The task was to model and store advertising event data in Apache Cassandra. 

### Schema (5 tables)
| Table | Query Pattern |
|---|---|
| `campaign_performance_by_day` | CTR per campaign per day |
| `advertiser_spend` | Top advertisers by spend in date range |
| `user_ad_history` | Last N ads seen by a user |
| `user_click_counts` | Most active users by click count |
| `advertiser_spend_by_region` | Top spenders in a specific region |

### Files
| File | Purpose |
|---|---|
| `sql/05_cassandra_schema.cql` | Creates keyspace and 5 tables |
| `sql/06_cassandra_queries.cql` | All 5 CQL queries |
| `scripts/06_load_cassandra.py` | Loads data from CSVs into Cassandra |
| `scripts/07_cassandra_queries.py` | Runs queries and exports JSON report |

### How to run
```bash
# Start Cassandra
docker compose up -d cassandra

# Wait ~2 minutes for Cassandra to become healthy, then create schema
docker exec -i adtech_cassandra cqlsh < sql/05_cassandra_schema.cql

# Install dependencies
pip3 install cassandra-driver

# Load data (takes ~30-60 minutes for 10M rows)
python3 scripts/06_load_cassandra.py

# Run queries and generate report
python3 scripts/07_cassandra_queries.py
```

---

## Homework 5: Real-Time Ad API Caching with Redis

The task was to build a simple REST API to serve some of the important analytics-related requests and implement the cache for the API.

### Endpoints
| Endpoint | Source | Cache TTL |
|---|---|---|
| `GET /campaign/{campaign_id}/performance` | MySQL | 30 seconds |
| `GET /advertiser/{advertiser_id}/spending` | MySQL | 5 minutes |
| `GET /user/{user_id}/engagements` | MongoDB | 2 minutes |
| `GET /health` | — | no cache |

### Benchmarking Results
| Endpoint | Without Cache | With Cache | Speedup |
|---|---|---|---|
| Campaign Performance | 208.1ms | 10.3ms | 20.2x |
| Advertiser Spending | 2770.4ms | 10.1ms | 274.3x |
| User Engagements | 56.3ms | 10.7ms | 5.3x |

### Files
| File | Purpose |
|---|---|
| `scripts/08_api.py` | FastAPI app with 3 endpoints and Redis caching |
| `scripts/09_benchmark.py` | Measures response times with and without cache |

### How to run
```bash
# Start Redis
docker compose up -d redis

# Install dependencies
pip3 install fastapi uvicorn redis

# Start the API
uvicorn scripts.08_api:app --reload --port 8000

# In a separate terminal, run the benchmark
python3 scripts/09_benchmark.py

# View auto-generated API docs
open http://127.0.0.1:8000/docs
```

---

## Full Stack Setup

To run all services at once:

```bash
docker compose up -d
```

Services and ports:
| Service | Port |
|---|---|
| MySQL | 3306 |
| MongoDB | 27017 |
| Cassandra | 9042 |
| Redis | 6379 |
| FastAPI | 8000 |

---

## License + Disclaimer
Everything is done for the homework purposes only (Data Engineering Micromasters, SET University, 2026)