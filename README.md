# 🌍 GDELT CAMEO — Real-Time Geopolitical Event ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-Latest-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![Podman](https://img.shields.io/badge/Podman-Container-892CA0?style=for-the-badge&logo=podman&logoColor=white)

> **Pipeline ETL end-to-end** yang mengambil data event geopolitik dunia dari **GDELT Project** secara real-time setiap 15 menit, mentransformasinya, lalu menyimpannya ke **PostgreSQL** dan memvisualisasikannya di **Grafana**.

---

## 📌 Daftar Isi

- [Gambaran Umum](#-gambaran-umum)
- [Arsitektur Pipeline](#-arsitektur-pipeline)
- [Tech Stack](#-tech-stack)
- [Struktur Proyek](#-struktur-proyek)
- [Prasyarat](#-prasyarat)
- [Cara Menjalankan](#-cara-menjalankan)
- [Konfigurasi Environment](#-konfigurasi-environment)
- [Penjelasan Pipeline](#-penjelasan-pipeline)
- [Akses Dashboard](#-akses-dashboard)
- [Screenshot](#-screenshot)
- [Pelajaran yang Dipetik](#-pelajaran-yang-dipetik)
- [Kontak](#-kontak)

---

## 🧭 Gambaran Umum

**GDELT (Global Database of Events, Language, and Tone)** adalah database terbuka terbesar di dunia yang memantau peristiwa global dari berbagai sumber berita. Project ini membangun pipeline data otomatis yang:

1. **Mengambil (Extract)** data terbaru dari GDELT API setiap 15 menit
2. **Mentransformasi (Transform)** data mentah menjadi format bersih dengan kolom-kolom esensial
3. **Memuat (Load)** data ke PostgreSQL sebagai data warehouse
4. **Memvisualisasikan** insight melalui dashboard Grafana secara real-time

### Mengapa Proyek Ini?

Proyek ini mendemonstrasikan kemampuan seorang **Data Engineer** dalam:

- Membangun pipeline ETL yang **terjadwal** dan **fault-tolerant**
- Melakukan orkestrasi workflow dengan **Apache Airflow**
- Mengelola infrastruktur data dengan **containerisasi** (Podman/Docker)
- Merancang skema data warehouse di **PostgreSQL**
- Membuat dashboard monitoring dengan **Grafana**

---

## 🏗 Arsitektur Pipeline

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   GDELT API     │────▶│  Apache Airflow │────▶│   PostgreSQL    │────▶│    Grafana      │
│   (Data Source) │     │  (Orchestrator) │     │ (Data Warehouse)│     │  (Visualization)│
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │                       │
   Data mentah            Extract &               Penyimpanan            Dashboard
   CSV terkompresi        Transform               terstruktur            real-time
   (setiap 15 menit)     (Pandas)                 (SQL Schema)           (Monitoring)
```

### Alur Data

```
GDELT lastupdate.txt → Download ZIP → Ekstrak CSV → Transformasi (Pandas) → Load ke PostgreSQL → Visualisasi di Grafana
```

---

## 🛠 Tech Stack

| Komponen | Teknologi | Fungsi |
|----------|-----------|--------|
| **Orchestrator** | Apache Airflow 2.7.1 | Penjadwalan & orkestrasi pipeline ETL |
| **Data Warehouse** | PostgreSQL 13 | Penyimpanan data terstruktur |
| **Visualization** | Grafana | Dashboard monitoring & analisis |
| **Container Runtime** | Podman (Docker-compatible) | Containerisasi seluruh infrastruktur |
| **Bahasa** | Python 3.8+ | Scripting ETL (Pandas, Requests) |
| **Data Source** | GDELT Project v2 | Sumber data event geopolitik global |

---

## 📂 Struktur Proyek

```
project2-gdeltcameo/
├── dags/
│   └── gdelt_pipeline.py      # DAG Airflow: logika ETL (extract, transform, load)
├── logs/                       # Log eksekusi Airflow (auto-generated)
├── plugins/                    # Plugin kustom Airflow (opsional)
├── docker-compose.yml          # Definisi layanan: Airflow, PostgreSQL, Grafana
├── .env                        # Variabel lingkungan (kredensial, konfigurasi)
├── .gitignore                  # Daftar file/folder yang diabaikan Git
└── README.md                   # Dokumentasi proyek (file ini)
```

---

## ⚙ Prasyarat

Pastikan tools berikut sudah terinstall di mesin Anda:

- **Podman** (atau Docker) — container runtime
- **Podman Compose** (atau Docker Compose) — orkestrasi multi-container
- **Git** — version control

```bash
# Cek instalasi
podman --version
podman-compose --version
git --version
```

---

## 🚀 Cara Menjalankan

### 1. Clone Repository

```bash
git clone https://github.com/<username>/gdelt-cameo-realtime-etl-pipeline.git
cd gdelt-cameo-realtime-etl-pipeline
```

### 2. Konfigurasi Environment

Buat file `.env` di root proyek:

```env
# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD= (sesuaikan)
POSTGRES_DB=airflow

# Airflow
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=(sesuaikan)
AIRFLOW_UID=50000

# Grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=(sesuaikan)
```

### 3. Jalankan Semua Service

```bash
podman-compose up -d
```

### 4. Buat Schema & Tabel di PostgreSQL

Setelah semua container berjalan, masuk ke container PostgreSQL dan buat schema:

```bash
podman exec -it <postgres_container_name> psql -U airflow -d airflow
```

```sql
CREATE SCHEMA IF NOT EXISTS gdelt;

CREATE TABLE IF NOT EXISTS gdelt.gdelt_events (
    globaleventid   BIGINT PRIMARY KEY,
    sqldate         DATE,
    actor1_name     VARCHAR(255),
    actor1_country  VARCHAR(10),
    actor2_name     VARCHAR(255),
    actor2_country  VARCHAR(10),
    cameo_code      VARCHAR(10),
    goldstein_scale FLOAT,
    num_mentions    INT,
    source_url      TEXT
);
```

### 5. Setup Airflow Connection

Buka Airflow UI di `http://localhost:8080`, lalu tambahkan connection PostgreSQL:

| Parameter | Nilai |
|-----------|-------|
| Connection Id | `postgres_default` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Database | `airflow` |
| Login | `airflow` |
| Password | `(sesuaikan)` |
| Port | `5432` |

### 6. Aktifkan DAG

Di Airflow UI, aktifkan DAG `gdelt_cameo_pipeline`. Pipeline akan berjalan otomatis **setiap 15 menit**.

### 7. Hentikan Service

```bash
podman-compose down
```

---

## 🔧 Konfigurasi Environment

| Variabel | Deskripsi | Default |
|----------|-----------|---------|
| `POSTGRES_USER` | Username database PostgreSQL | `airflow` |
| `POSTGRES_PASSWORD` | Password database PostgreSQL | `(sesuaikan)` |
| `POSTGRES_DB` | Nama database PostgreSQL | `airflow` |
| `AIRFLOW_UID` | UID untuk Airflow (penting di Linux) | `50000` |
| `GRAFANA_ADMIN_USER` | Username admin Grafana | `admin` |
| `GRAFANA_ADMIN_PASSWORD` | Password admin Grafana | `(sesuaikan)` |

---

## 🔄 Penjelasan Pipeline

Pipeline dijalankan oleh **Apache Airflow** dengan dua task utama:

### Task 1: `extract_transform_task`

1. Mengambil URL file CSV terbaru dari GDELT (`lastupdate.txt`)
2. Mengunduh file ZIP yang berisi data event
3. Mengekstrak dan membaca CSV dengan Pandas
4. Memilih 10 kolom esensial dari 60+ kolom yang tersedia:

| Kolom | Deskripsi |
|-------|-----------|
| `GlobalEventID` | ID unik event global |
| `Day` | Tanggal event (dikonversi ke format DATE) |
| `Actor1Name` | Nama aktor pertama |
| `Actor1CountryCode` | Kode negara aktor pertama |
| `Actor2Name` | Nama aktor kedua |
| `Actor2CountryCode` | Kode negara aktor kedua |
| `EventBaseCode` | Kode CAMEO event |
| `GoldsteinScale` | Skala Goldstein (-10 s/d +10) |
| `NumMentions` | Jumlah penyebutan di media |
| `SOURCEURL` | URL sumber berita |

5. Menyimpan hasil transformasi sebagai CSV sementara

### Task 2: `load_to_postgres_task`

1. Membaca file CSV sementara dari task sebelumnya (via XCom)
2. Menggunakan `PostgresHook` untuk koneksi ke database
3. Memuat data dengan `COPY` command untuk performa optimal

### Orkestrasi

```
extract_transform_task → load_to_postgres_task
```

Pipeline dijadwalkan dengan `schedule_interval='*/15 * * * *'` (setiap 15 menit).

---

## 📊 Akses Dashboard

| Service | URL | Username | Password |
|---------|-----|----------|----------|
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `admin` | `(sesuaikan)` |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` | `(sesuaikan)` |
| **PostgreSQL** | `localhost:5432` | `airflow` | `(sesuaikan)` |

### Setup Grafana Data Source

1. Buka Grafana → **Configuration** → **Data Sources**
2. Pilih **PostgreSQL**
3. Isi konfigurasi:
   - **Host**: `postgres:5432`
   - **Database**: `airflow`
   - **User**: `airflow`
   - **Password**: `(sesuaikan)`
   - **SSL Mode**: `disable`
4. Klik **Save & Test**

---

## 📸 Screenshot

> <img width="2370" height="1111" alt="image" src="https://github.com/user-attachments/assets/8574183b-d5dd-4677-b8d3-4bc0d55c3d63" />


Link Grafana: https://snapshots.raintank.io/dashboard/snapshot/f09izpGnSJ1peEWrgPCE37TzXoT2s52Y

<!--
![Airflow DAG](docs/screenshots/airflow-dag.png)
![Grafana Dashboard](docs/screenshots/grafana-dashboard.png)
![PostgreSQL Data](docs/screenshots/postgres-data.png)
-->

---

## 💡 Pelajaran yang Dipetik

- **Containerisasi** sangat mempermudah setup infrastruktur data yang kompleks
- **Apache Airflow** menyediakan mekanisme retry & monitoring yang handal untuk pipeline ETL
- Penggunaan `COPY` pada PostgreSQL jauh lebih cepat dibanding `INSERT` row-by-row
- Pentingnya **idempotent pipeline** agar data tidak duplikat saat re-run
- Pengelolaan **environment variables** dengan `.env` menjaga keamanan kredensial

---

## 📬 Kontak

**Dinar Rahman** — Data Engineer

<!-- Sesuaikan link berikut dengan profil Anda -->
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/dinar-wahyu-rahman)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/dinarrahman30)
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:dinarrahman30)

---

<p align="center">
  <i>Dibuat dengan ❤️ sebagai portfolio project Data Engineer</i>
</p>
