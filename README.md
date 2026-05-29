LINK DOCS: https://docs.google.com/document/d/1QNQnLpkONlkmrnC1jq2ElR96eKnCKnLs-Ys8RvTSqVs/edit?usp=sharing

# 🌾 HTAP Smart Farming Experiment

**Analisis Performa Arsitektur HTAP untuk Prediksi Gagal Panen dan Pencatatan Logistik Real-Time**

> Tugas Besar Manajemen Basis Data (IF25-40405) — Institut Teknologi Sumatera, 2026  
> Dosen Pengampu: Meida Cahyo Untoro, S.Kom., M.Kom.

---

## 📖 Deskripsi Proyek

Repositori ini berisi skrip eksperimen untuk mengevaluasi performa arsitektur **Hybrid Transactional/Analytical Processing (HTAP)** berbasis **TiDB Cloud** pada skenario _smart farming_. Penelitian ini membandingkan dua arsitektur:

| Arsitektur | Deskripsi |
|---|---|
| **HTAP (TiDB)** | OLTP via TiKV (row store) + OLAP via TiFlash (columnar store) dengan replikasi Multi-Raft asinkron dan optimasi teknik **Diva** (Decoupling Index from Version Data) |
| **Tradisional (Baseline)** | OLTP dan OLAP dipaksa berjalan pada jalur penyimpanan TiKV (row store) yang sama — mensimulasikan arsitektur konvensional tanpa isolasi beban kerja |

Eksperimen ini secara khusus menguji tiga hipotesis utama:
1. TiFlash menghasilkan latensi kueri analitik lebih rendah dan stabil dibanding row store.
2. MVCC + Diva menekan abort rate mendekati 0% pada beban kerja campuran.
3. Replikasi Multi-Raft mempertahankan data freshness di bawah 1 detik tanpa pipeline ETL.

---

## 📁 Struktur File

```
htap_experiment/
├── testing.py                  # Skenario HTAP: OLTP (TiKV) + OLAP (TiFlash/Diva) secara simultan
├── tradisional_Testing.py      # Skenario Baseline: OLTP + OLAP dipaksa ke TiKV (row store)
├── crop_recommendation.csv     # Dataset sensor IoT pertanian (2.200 baris, 8 kolom)
├── yield_history.csv           # Dataset historis hasil panen (multi-negara, multi-tahun)
└── README.md                   # Dokumentasi ini
```

### Dataset

**`crop_recommendation.csv`** — Sumber beban kerja OLTP (simulasi data sensor IoT)
| Kolom | Tipe | Keterangan |
|---|---|---|
| `N` | float | Kandungan Nitrogen tanah |
| `P` | float | Kandungan Fosfor tanah |
| `K` | float | Kandungan Kalium tanah |
| `temperature` | float | Suhu udara (°C) |
| `humidity` | float | Kelembapan udara (%) |
| `ph` | float | Tingkat keasaman tanah |
| `rainfall` | float | Curah hujan (mm) |
| `label` | string | Jenis tanaman yang direkomendasikan |

**`yield_history.csv`** — Sumber beban kerja OLAP (data historis untuk prediksi gagal panen)
| Kolom | Tipe | Keterangan |
|---|---|---|
| `No` | int | ID record |
| `Area` | string | Nama negara/wilayah |
| `Item` | string | Nama komoditas tanaman |
| `Year` | int | Tahun periode panen |
| `hg/ha_yield` | float | Hasil panen (hektogram per hektare) |
| `average_rain_fall_mm_per_year` | float | Rata-rata curah hujan tahunan |
| `pesticides_tonnes` | float | Penggunaan pestisida (ton) |
| `avg_temp` | float | Suhu rata-rata (°C) |

---

## ⚙️ Prasyarat & Instalasi

### Dependensi Python

```bash
pip install pymysql pandas matplotlib
```

### Konfigurasi Database (TiDB Cloud)

Sebelum menjalankan skrip, buat akun di [TiDB Cloud](https://tidbcloud.com) dan buat cluster **Serverless (Starter Plan)** di region **AWS Singapore**. Kemudian perbarui variabel `DB_CONFIG` di kedua skrip:

```python
DB_CONFIG = {
    'host':     '<YOUR_TIDB_HOST>',       # Contoh: gateway01.ap-southeast-1.prod.aws.tidbcloud.com
    'port':     4000,
    'user':     '<YOUR_USERNAME>',
    'password': '<YOUR_PASSWORD>',
    'database': 'smart_farming_db',
    'autocommit': True,
    'ssl': {'ssl_disabled': False}
}
```

> ⚠️ **Penting:** Jangan commit kredensial database ke repositori publik. Gunakan variabel lingkungan atau file `.env`.

### Setup Skema Database

Jalankan DDL berikut di TiDB Cloud SQL Editor (atau Chat2Query) untuk menyiapkan tabel:

```sql
CREATE DATABASE IF NOT EXISTS smart_farming_db;
USE smart_farming_db;

-- Tabel OLTP: data sensor IoT real-time
CREATE TABLE sensor_iot (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    N           FLOAT,
    P           FLOAT,
    K           FLOAT,
    temperature FLOAT,
    humidity    FLOAT,
    ph          FLOAT,
    rainfall    FLOAT,
    label       VARCHAR(50),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabel OLAP: data historis hasil panen
CREATE TABLE yield_history (
    id                              BIGINT AUTO_INCREMENT PRIMARY KEY,
    area                            VARCHAR(100),
    item                            VARCHAR(100),
    year                            INT,
    `hg/ha_yield`                   FLOAT,
    average_rain_fall_mm_per_year   FLOAT,
    pesticides_tonnes               FLOAT,
    avg_temp                        FLOAT
);

-- Aktifkan replikasi TiFlash untuk kedua tabel (wajib untuk skenario HTAP)
ALTER TABLE sensor_iot  SET TIFLASH REPLICA 2;
ALTER TABLE yield_history SET TIFLASH REPLICA 2;
```

Impor data `yield_history.csv` ke tabel `yield_history` melalui fitur **Data Import** di TiDB Cloud sebelum menjalankan eksperimen.

---

## 🚀 Cara Menjalankan Eksperimen

### Skenario 1 — HTAP (testing.py)

Menjalankan OLTP dan OLAP secara **simultan** dengan isolasi beban kerja TiKV/TiFlash:

```bash
python testing.py
```

**Alur eksperimen (durasi default: 30 detik):**
- Thread 1 (`oltp_worker`): membaca `crop_recommendation.csv` dan meng-ingest data ke tabel `sensor_iot` via TiKV dalam batch 50 baris setiap 2 detik.
- Thread 2 (`olap_worker`): menjalankan kueri agregasi `AVG(hg/ha_yield) GROUP BY item` pada `yield_history` via TiFlash menggunakan hint `READ_FROM_STORAGE(tiflash[...])` setiap 5 detik.

**Output:**
- Metrik evaluasi dicetak ke konsol (TPS, latensi OLAP, data freshness, abort rate).
- Grafik disimpan sebagai `hasil_eksperimen_htap.png`.

---

### Skenario 2 — Tradisional/Baseline (tradisional_Testing.py)

Menjalankan OLTP dan OLAP secara simultan **tanpa isolasi**, keduanya dipaksa ke TiKV:

```bash
python tradisional_Testing.py
```

**Alur eksperimen (durasi default: 60 detik):**
- Thread 1 (`oltp_worker`): ingest data sensor ke TiKV dalam batch 50 baris setiap 1 detik (lebih agresif untuk memaksimalkan resource contention).
- Thread 2 (`olap_traditional_worker`): menjalankan kueri agregasi yang sama tetapi dipaksa ke TiKV via hint `READ_FROM_STORAGE(tikv[...])` tanpa jeda (stress test penuh).

**Output:**
- Metrik evaluasi (TPS, latensi min/avg/max, data freshness, abort rate) dicetak ke konsol.
- Grafik disimpan sebagai `hasil_eksperimen_tradisional.png`.

---

## 📊 Metrik Evaluasi

| Metrik | Deskripsi | Target |
|---|---|---|
| **Throughput (TPS)** | Jumlah transaksi penulisan sensor berhasil per detik | Stabil dan tinggi |
| **Query Latency (ms)** | Waktu eksekusi kueri analitik OLAP | Rendah dan konsisten |
| **Data Freshness (detik)** | Jeda antara data ditulis di TiKV hingga tersedia di TiFlash | **< 1 detik** |
| **Abort Rate (%)** | Persentase transaksi gagal akibat konflik konkurensi | **Mendekati 0%** |

---

## 🏗️ Arsitektur Sistem

```
┌─────────────────────────────────────────────────────┐
│               TiDB Cloud Serverless                 │
│  ┌─────────────────────────────────────────────┐    │
│  │            TiDB Server (SQL Layer)          │    │
│  └────────────┬──────────────────┬─────────────┘    │
│               │ OLTP             │ OLAP              │
│  ┌────────────▼──────┐  ┌───────▼───────────────┐   │
│  │   TiKV Cluster    │  │    TiFlash Cluster     │   │
│  │  (Row Store)      │◄─►  (Columnar Store)      │   │
│  │  sensor_iot       │  │  yield_history         │   │
│  │  yield_history    │  │  sensor_iot            │   │
│  └───────────────────┘  └────────────────────────┘   │
│         ▲  Multi-Raft Async Replication ▲            │
│  ┌──────┴──────────────────────────────┴──────────┐  │
│  │         Placement Driver (PD)                  │  │
│  │  Timestamp Oracle (TSO) · Load Balancing       │  │
│  └────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
         ▲                           ▲
   crop_recommendation.csv    yield_history.csv
   (OLTP: Python ingest)      (OLAP: Analytics)
```

---

## 👥 Tim Peneliti

| Nama | NIM |
|---|---|
| Garis Rayya Rabbani | 123140018 |
| Arrauf Setiawan Muhammad Jabar | 123140032 |
| Aryasatya Widyatna Akbar | 123140164 |
| Muhammad Fauzan Naufal | 123140150 |
| Bagas Dwi Ajitya | 123140181 |
| Aprililianti | 123140041 |
| Bima Aryaseta | 123140177 |
| Muhammad Arkan Saktiawan | 123140166 |

**Program Studi Teknik Informatika — Fakultas Teknologi Industri**  
**Institut Teknologi Sumatera (ITERA), 2026**

---

## 📚 Referensi Utama

- Kim et al. (2022). *Diva: Making MVCC Systems HTAP-Friendly.* ACM SIGMOD.
- Kishore & Yoo (2025). *Practicing HTAP in the Cloud: Evaluating TiDB.* IEEE BigData.
- Huang et al. (2022). *Opportunities for Optimism in Contended Main-Memory Multicore Transactions.* VLDB Journal.
- Hieber & Grambow (2020). *Hybrid Transactional and Analytical Processing Databases: A Systematic Literature Review.*
