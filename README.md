# 🌍 GDELT CAMEO Realtime ETL Pipeline

<div align="center">

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Latest-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Podman](https://img.shields.io/badge/Podman-Container-892CA0?style=for-the-badge&logo=podman&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-ETL-150458?style=for-the-badge&logo=pandas&logoColor=white)

**Realtime ETL pipeline yang mengekstrak data geopolitik global dari GDELT Project, mentransformasi kode CAMEO, dan memuat ke PostgreSQL — dijalankan otomatis setiap 15 menit via Apache Airflow.**

</div>

---

## 📖 Deskripsi Proyek

**GDELT (Global Database of Events, Language, and Tone)** adalah dataset publik yang merekam seluruh kejadian global berbasis media berita di seluruh dunia. Dataset ini diperbarui **setiap 15 menit** dan menggunakan standar kode **CAMEO (Conflict and Mediation Event Observations)** untuk mengklasifikasikan jenis interaksi antar aktor (negara, tokoh, organisasi).

Pipeline ini dirancang untuk keperluan analitik geopolitik realtime dengan alur:

```
GDELT API → Extract → Transform (CAMEO) → Load → PostgreSQL → Grafana Dashboard
```

### 🎯 Tujuan Proyek
- Membangun pipeline ETL otomatis berbasis event dari sumber data publik skala besar
- Mendemonstrasikan kemampuan orkestrasi workflow dengan Apache Airflow
- Menyediakan data siap analisis untuk monitoring eskalasi konflik global

---

## 🏗️ Arsitektur

```
┌─────────────────────────────────────────────────────────────┐
│                        Podman / Docker                       │
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
│                              │  Dashboard       │            │
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

## 🔄 Alur ETL Pipeline

### 1. Extract
- Mengambil URL file terbaru dari `http://data.gdeltproject.org/gdeltv2/lastupdate.txt`
- Mengunduh file `.export.CSV.zip` dan mengekstraknya secara in-memory (tanpa menyimpan ke disk)

### 2. Transform
Memilih 10 kolom relevan dari 57+ kolom GDELT dan melakukan normalisasi:

| Kolom Output | Kolom GDELT | Keterangan |
|---|---|---|
| `GlobalEventID` | Index 0 | ID unik event |
| `Day` | Index 1 | Tanggal event (format: YYYY-MM-DD) |
| `Actor1Name` | Index 6 | Nama aktor 1 |
| `Actor1CountryCode` | Index 7 | Kode negara aktor 1 |
| `Actor2Name` | Index 16 | Nama aktor 2 |
| `Actor2CountryCode` | Index 17 | Kode negara aktor 2 |
| `EventBaseCode` | Index 27 | Kode CAMEO (zero-padded) |
| `GoldsteinScale` | Index 30 | Skala dampak (-10 hingga +10) |
| `NumMentions` | Index 31 | Jumlah penyebutan di media |
| `SOURCEURL` | Index 60 | URL sumber berita |

### 3. Load
- Memuat data bersih ke PostgreSQL menggunakan `COPY` command via `PostgresHook`
- Target tabel: `gdelt.gdelt_events`

---

## 🛠️ Tech Stack

| Komponen | Teknologi | Peran |
|---|---|---|
| Orkestrasi | Apache Airflow 2.7.1 | Penjadwalan & monitoring pipeline |
| Penyimpanan | PostgreSQL 13 | Data warehouse |
| Visualisasi | Grafana | Dashboard analitik |
| Transformasi | Python (Pandas) | ETL & data cleaning |
| Container | Podman / Docker | Isolasi & deployment |
| Sumber Data | GDELT Project v2 | Public event dataset |

---

## 📁 Struktur Direktori

```
project2-gdeltcameo/
├── dags/
│   └── gdelt_pipeline.py      # DAG utama: Extract, Transform, Load
├── logs/                      # Log Airflow (auto-generated)
├── plugins/                   # Custom Airflow plugins
├── docker-compose.yml         # Definisi layanan (Airflow, Postgres, Grafana)
├── .env                       # Konfigurasi environment (tidak di-commit)
├── .gitignore
└── README.md
```

---

## ⚙️ Cara Menjalankan

### Prasyarat
- [Podman](https://podman.io/) atau [Docker](https://www.docker.com/) terinstall
- `podman-compose` / `docker compose` tersedia
- Python 3.x (opsional, untuk pengembangan lokal)

### 1. Clone Repository

```bash
git clone https://github.com/<username>/project2-gdeltcameo.git
cd project2-gdeltcameo
```

### 2. Konfigurasi Environment

Buat file `.env` berdasarkan template berikut:

```env
# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=<your_password>
POSTGRES_DB=airflow

# Airflow
AIRFLOW_UID=50000

# Grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=<your_password>
```

### 3. Jalankan Services

```bash
# Menggunakan Podman
podman-compose up -d

# Atau menggunakan Docker
docker compose up -d
```

### 4. Akses Services

| Service | URL | Kredensial Default |
|---|---|---|
| Airflow Webserver | http://localhost:8080 | admin / air1234 |
| Grafana Dashboard | http://localhost:3000 | admin / air12345 |
| PostgreSQL | localhost:5432 | Lihat `.env` |

### 5. Konfigurasi Koneksi Airflow

Di Airflow UI, buat koneksi PostgreSQL:
- **Connection ID**: `postgres_default`
- **Host**: `postgres`
- **Port**: `5432`
- **Database**: nilai `POSTGRES_DB` dari `.env`
- **Login/Password**: nilai dari `.env`

### 6. Aktifkan DAG

Di Airflow UI, aktifkan DAG `gdelt_cameo_pipeline`. Pipeline akan berjalan otomatis setiap **15 menit**.

---

## 🗄️ Skema Database

```sql
CREATE TABLE IF NOT EXISTS gdelt_events (
    globaleventid     TEXT,
    sqldate           DATE,
    actor1_name       TEXT,
    actor1_country    TEXT,
    actor2_name       TEXT,
    actor2_country    TEXT,
    cameo_code        TEXT,   -- Kode CAMEO (mis. "01" = Public Statement)
    goldstein_scale   TEXT,   -- Skala -10 (konflik) hingga +10 (kooperatif)
    num_mentions      TEXT,
    source_url        TEXT
);
```

### Tentang Kode CAMEO

Kode CAMEO mengklasifikasikan jenis interaksi geopolitik. Contoh:

| Kode | Kategori |
|---|---|
| 01 | Public Statement |
| 05 | Appeal |
| 10 | Demand |
| 14 | Protest |
| 18 | Assault |
| 19 | Fight |
| 20 | Use of Unconventional Mass Violence |

---

## 📊 Contoh Query Analitik

```sql
-- Top 10 negara yang paling sering muncul sebagai aktor
SELECT actor1_country, COUNT(*) as event_count
FROM gdelt.gdelt_events
WHERE actor1_country IS NOT NULL AND actor1_country != ''
GROUP BY actor1_country
ORDER BY event_count DESC
LIMIT 10;

-- Distribusi Goldstein Scale (tingkat kooperasi vs konflik)
SELECT
    CASE
        WHEN goldstein_scale::FLOAT > 0 THEN 'Kooperatif'
        WHEN goldstein_scale::FLOAT < 0 THEN 'Konfliktual'
        ELSE 'Netral'
    END AS kategori,
    COUNT(*) as jumlah
FROM gdelt.gdelt_events
WHERE goldstein_scale ~ '^-?[0-9.]+$'
GROUP BY kategori;

-- Tren event terbaru dalam 24 jam terakhir
SELECT sqldate, COUNT(*) as total_events
FROM gdelt.gdelt_events
WHERE sqldate >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY sqldate
ORDER BY sqldate DESC;
```

---

## 🚀 Pengembangan Selanjutnya

- [ ] Tambahkan skema `gdelt` yang terpisah di PostgreSQL
- [ ] Implementasi deduplication berdasarkan `GlobalEventID`
- [ ] Integrasi tabel referensi kode CAMEO untuk enrichment data
- [ ] Bangun Grafana dashboard: peta panas konflik global
- [ ] Tambahkan alerting jika Goldstein Scale memburuk secara signifikan
- [ ] Migrasi ke distributed storage (e.g., MinIO + Apache Parquet)

---

## 👤 Author

**Dinar Rahman**
- Data Engineer Portfolio Project
- Fokus: Realtime Data Pipeline, Event-Driven Architecture, Geopolitical Analytics

---

## 📄 Lisensi

Proyek ini menggunakan lisensi [MIT](LICENSE).

Data bersumber dari **GDELT Project** yang merupakan dataset publik:
> The GDELT Project is an open platform for research and analysis of global society.
> Learn more at [gdeltproject.org](https://www.gdeltproject.org/)
