# 🌍 GDELT CAMEO Realtime ETL Pipeline

<div align="center">

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Latest-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Podman](https://img.shields.io/badge/Podman-Container-892CA0?style=for-the-badge&logo=podman&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-ETL-150458?style=for-the-badge&logo=pandas&logoColor=white)

**A realtime ETL pipeline that extracts global geopolitical event data from the GDELT Project, transforms CAMEO event codes, and loads them into PostgreSQL — automatically scheduled every 15 minutes via Apache Airflow.**

</div>

---

## 📖 Project Overview

**GDELT (Global Database of Events, Language, and Tone)** is a public dataset that records global events from news media worldwide. The dataset is updated **every 15 minutes** and uses the **CAMEO (Conflict and Mediation Event Observations)** coding system to classify types of interactions between political actors (countries, leaders, organizations).

This pipeline is designed for realtime geopolitical analytics:

```
GDELT API → Extract → Transform (CAMEO) → Load → PostgreSQL → Grafana Dashboard
```

### 🎯 Project Goals
- Build an automated event-driven ETL pipeline from a large-scale public data source
- Demonstrate workflow orchestration skills with Apache Airflow
- Provide analysis-ready data for monitoring global conflict escalation trends

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Podman / Docker                        │
│                                                             │
│  ┌──────────────────┐    ┌──────────────────────────────┐  │
│  │  Apache Airflow  │    │         PostgreSQL 13         │  │
│  │  (Port 8080)     │───▶│  Schema: gdelt               │  │
│  │                  │    │  Table:  gdelt_events         │  │
│  │  DAG: runs every │    │  (Port 5432)                  │  │
│  │  15 minutes      │    └──────────────┬───────────────┘  │
│  └──────────────────┘                   │                   │
│                                         ▼                   │
│                              ┌─────────────────┐            │
│                              │     Grafana      │            │
│                              │   Dashboard      │            │
│                              │  (Port 3000)     │            │
│                              └─────────────────┘            │
└─────────────────────────────────────────────────────────────┘
         ▲
         │ HTTP (ZIP/CSV)
┌────────┴────────┐
│  GDELT Project  │
│  (Public API)   │
│  Update: /15min │
└─────────────────┘
```

---

## 🔄 ETL Pipeline Flow

### 1. Extract
- Fetches the latest file URL from `http://data.gdeltproject.org/gdeltv2/lastupdate.txt`
- Downloads the `.export.CSV.zip` file and extracts it **in-memory** (no intermediate disk writes)

### 2. Transform
Selects 10 relevant columns from 57+ GDELT columns and applies normalization:

| Output Column | GDELT Index | Description |
|---|---|---|
| `GlobalEventID` | 0 | Unique event identifier |
| `Day` | 1 | Event date (YYYY-MM-DD) |
| `Actor1Name` | 6 | Name of Actor 1 |
| `Actor1CountryCode` | 7 | Country code of Actor 1 |
| `Actor2Name` | 16 | Name of Actor 2 |
| `Actor2CountryCode` | 17 | Country code of Actor 2 |
| `EventBaseCode` | 27 | CAMEO code (zero-padded) |
| `GoldsteinScale` | 30 | Impact scale (-10 to +10) |
| `NumMentions` | 31 | Number of media mentions |
| `SOURCEURL` | 60 | Source news article URL |

### 3. Load
- Loads cleaned data into PostgreSQL using the `COPY` command via Airflow's `PostgresHook`
- Target table: `gdelt.gdelt_events`

---

## 🛠️ Tech Stack

| Component | Technology | Role |
|---|---|---|
| Orchestration | Apache Airflow 2.7.1 | Pipeline scheduling & monitoring |
| Storage | PostgreSQL 13 | Data warehouse |
| Visualization | Grafana | Analytics dashboard |
| Transformation | Python (Pandas) | ETL & data cleaning |
| Containerization | Podman / Docker | Isolated deployment |
| Data Source | GDELT Project v2 | Public event dataset |

---

## 📁 Project Structure

```
project2-gdeltcameo/
├── dags/
│   └── gdelt_pipeline.py      # Main DAG: Extract, Transform, Load
├── logs/                      # Airflow logs (auto-generated)
├── plugins/                   # Custom Airflow plugins
├── docker-compose.yml         # Service definitions (Airflow, Postgres, Grafana)
├── .env                       # Environment configuration (not committed)
├── .gitignore
├── README.md                  # Documentation (Bahasa Indonesia)
└── README.en.md               # Documentation (English)
```

---

## ⚙️ Getting Started

### Prerequisites
- [Podman](https://podman.io/) or [Docker](https://www.docker.com/) installed
- `podman-compose` or `docker compose` available
- Python 3.x (optional, for local development)

### 1. Clone the Repository

```bash
git clone https://github.com/<username>/project2-gdeltcameo.git
cd project2-gdeltcameo
```

### 2. Configure Environment Variables

Create a `.env` file based on the following template:

```env
# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=<your_secure_password>
POSTGRES_DB=airflow

# Airflow (important for Linux permission handling)
AIRFLOW_UID=50000

# Grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<your_secure_password>
```

### 3. Start All Services

```bash
# Using Podman
podman-compose up -d

# Or using Docker
docker compose up -d
```

### 4. Access the Services

| Service | URL | Default Credentials |
|---|---|---|
| Airflow Webserver | http://localhost:8080 | admin / air1234 |
| Grafana Dashboard | http://localhost:3000 | admin / air12345 |
| PostgreSQL | localhost:5432 | See `.env` |

### 5. Configure Airflow Connection

In the Airflow UI, create a new PostgreSQL connection:
- **Connection ID**: `postgres_default`
- **Host**: `postgres`
- **Port**: `5432`
- **Database**: value of `POSTGRES_DB` from `.env`
- **Login / Password**: values from `.env`

### 6. Enable the DAG

In the Airflow UI, toggle on the `gdelt_cameo_pipeline` DAG. The pipeline will run automatically **every 15 minutes**.

---

## 🗄️ Database Schema

```sql
CREATE TABLE IF NOT EXISTS gdelt_events (
    globaleventid     TEXT,
    sqldate           DATE,
    actor1_name       TEXT,
    actor1_country    TEXT,
    actor2_name       TEXT,
    actor2_country    TEXT,
    cameo_code        TEXT,   -- CAMEO code (e.g. "01" = Public Statement)
    goldstein_scale   TEXT,   -- Scale: -10 (conflict) to +10 (cooperative)
    num_mentions      TEXT,
    source_url        TEXT
);
```

### About CAMEO Codes

CAMEO codes classify the type of geopolitical interaction. Key examples:

| Code | Category |
|---|---|
| 01 | Public Statement |
| 05 | Appeal |
| 10 | Demand |
| 14 | Protest |
| 18 | Assault |
| 19 | Fight |
| 20 | Use of Unconventional Mass Violence |

Full CAMEO codebook: [cameonet.weebly.com](http://cameonet.weebly.com/)

---

## 📊 Sample Analytical Queries

```sql
-- Top 10 countries most frequently appearing as primary actors
SELECT actor1_country, COUNT(*) AS event_count
FROM gdelt.gdelt_events
WHERE actor1_country IS NOT NULL AND actor1_country != ''
GROUP BY actor1_country
ORDER BY event_count DESC
LIMIT 10;

-- Distribution of Goldstein Scale (cooperative vs. conflictual)
SELECT
    CASE
        WHEN goldstein_scale::FLOAT > 0 THEN 'Cooperative'
        WHEN goldstein_scale::FLOAT < 0 THEN 'Conflictual'
        ELSE 'Neutral'
    END AS category,
    COUNT(*) AS total
FROM gdelt.gdelt_events
WHERE goldstein_scale ~ '^-?[0-9.]+$'
GROUP BY category;

-- Recent event trend in the last 24 hours
SELECT sqldate, COUNT(*) AS total_events
FROM gdelt.gdelt_events
WHERE sqldate >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY sqldate
ORDER BY sqldate DESC;
```

---

## 🚀 Roadmap

- [ ] Add a dedicated `gdelt` schema in PostgreSQL
- [ ] Implement deduplication logic based on `GlobalEventID`
- [ ] Integrate CAMEO reference table for event code enrichment
- [ ] Build a Grafana dashboard: global conflict heatmap
- [ ] Add alerting when Goldstein Scale deteriorates significantly
- [ ] Migrate to distributed storage (e.g., MinIO + Apache Parquet)
- [ ] Add data quality checks with Great Expectations

---

## 👤 Author

**Dinar Rahman**
- Data Engineer Portfolio Project
- Focus: Realtime Data Pipelines, Event-Driven Architecture, Geopolitical Analytics

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

Data sourced from the **GDELT Project**, a public open-access dataset:
> The GDELT Project is an open platform for research and analysis of global society.
> Learn more at [gdeltproject.org](https://www.gdeltproject.org/)
