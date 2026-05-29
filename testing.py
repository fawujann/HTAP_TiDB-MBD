import pymysql
import pandas as pd
import threading
import time
import sys
import matplotlib.pyplot as plt

# === CONFIGURATION (Gunakan data cluster kamu) ===
DB_CONFIG = {
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'port': 4000,
    'user': '2qEWFvfev7wavRf.root',
    'password': 'NIbLpVSJekvn2Sb6',
    'database': 'smart_farming_db',
    'autocommit': True,
    'ssl': {'ssl_disabled': False}
}

def connect_db():
    try:
        return pymysql.connect(**DB_CONFIG)
    except Exception as e:
        print(f"❌ Gagal koneksi: {e}")
        sys.exit()

# Flag dan penanda waktu eksperimen
stop_experiment = False
start_experiment_time = None

# === STRUKTUR DATA UNTUK VISUALISASI ASLI ===
oltp_timestamps = []
oltp_total_rows = []

olap_timestamps = []
olap_latencies = []

# === VARIABEL TAMBAHAN UNTUK METRIK TEKS ===
total_success_transactions = 0
total_failed_transactions = 0
olap_freshness_history = []

# === SKENARIO 1: OLTP WRITE-INTENSIVE (METRIK: THROUGHPUT & ABORT RATE) ===
def oltp_worker():
    global start_experiment_time, total_success_transactions, total_failed_transactions
    conn = connect_db()
    cursor = conn.cursor()
    print("🚀 OLTP: Memulai Ingest Data Sensor (Simulasi IoT Real-Time)...")
    
    total_inserted = 0 # Counter akumulasi baris terinput
    try:
        df = pd.read_csv('crop_recommendation.csv')
        batch_size = 50 
        
        while not stop_experiment:
            for i in range(0, len(df), batch_size):
                if stop_experiment: break
                
                batch = df.iloc[i : i + batch_size]
                data = [tuple(x) for x in batch[['N', 'P', 'K', 'temperature', 'humidity', 'ph', 'rainfall', 'label']].values]
                
                sql = """INSERT INTO sensor_iot (N, P, K, temperature, humidity, ph, rainfall, label) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                
                try:
                    cursor.executemany(sql, data)
                    total_success_transactions += 1
                    
                    # Update data pencatatan visualisasi asli
                    total_inserted += len(data)
                    current_time = time.time() - start_experiment_time
                    oltp_timestamps.append(current_time)
                    oltp_total_rows.append(total_inserted)
                    
                    print(f"✅ [OLTP] Berhasil mengunggah baris ke-{i}... Total Akumulasi: {total_inserted}")
                
                except (pymysql.OperationalError, pymysql.InternalError) as e:
                    total_failed_transactions += 1
                    print(f"⚠️ [OLTP Aborted] Transaksi gagal akibat konflik konkurensi: {e}")
                
                time.sleep(2) 
    except Exception as e:
        print(f"❌ OLTP Error Utama: {e}")
    finally:
        conn.close()

# === SKENARIO 2: OLAP ANALYTIC (METRIK: QUERY LATENCY & DATA FRESHNESS) ===
def olap_worker():
    global start_experiment_time
    conn = connect_db()
    cursor = conn.cursor()
    print("📊 OLAP: Memulai Kueri Analitik Prediksi (Jalur Diva/TiFlash)...")
    try:
        while not stop_experiment:
            start_time = time.time()
            
            sql_olap = """SELECT /*+ READ_FROM_STORAGE(tiflash[yield_history]) */ 
                          item, AVG(`hg/ha_yield`) as avg_yield 
                          FROM yield_history GROUP BY item;"""
            
            cursor.execute(sql_olap)
            cursor.fetchall()
            
            latency = (time.time() - start_time) * 1000
            current_time = time.time() - start_experiment_time
            
            # Hitung Data Freshness (Hanya untuk metrik teks akhir)
            sql_freshness = """SELECT /*+ READ_FROM_STORAGE(tiflash[sensor_iot]) */ 
                               TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) as freshness_seconds 
                               FROM sensor_iot;"""
            try:
                cursor.execute(sql_freshness)
                freshness_result = cursor.fetchone()
                data_freshness = float(freshness_result[0]) if freshness_result and freshness_result[0] is not None else 0.0
            except Exception:
                data_freshness = 0.0
            
            # Catat data ke array masing-masing
            olap_timestamps.append(current_time)
            olap_latencies.append(latency)
            olap_freshness_history.append(data_freshness)
            
            print(f"⏱️ [OLAP] Kueri Analitik Selesai. Latensi: {latency:.2f}ms")
            
            time.sleep(5)
    except Exception as e:
        print(f"❌ OLAP Error: {e}")
    finally:
        conn.close()

# === MAIN EXECUTION: SKENARIO CAMPURAN (HTAP) ===
if __name__ == "__main__":
    start_experiment_time = time.time()
    
    t1 = threading.Thread(target=oltp_worker)
    t2 = threading.Thread(target=olap_worker)
    
    t1.start()
    t2.start()
    
    try:
        # Jalankan pengujian selama 120 detik
        time.sleep(30)
    except KeyboardInterrupt:
        print("\n⏹️ Eksperimen dihentikan oleh pengguna.")
    
    stop_experiment = True
    t1.join()
    t2.join()
    
    # === OUTPUT AKHIR METRIK EVALUASI (Teks di Konsol) ===
    print("\n================ EVALUASI METRIK SISTEM ================")
    total_transactions = total_success_transactions + total_failed_transactions
    abort_rate = (total_failed_transactions / total_transactions) * 100 if total_transactions > 0 else 0
    final_tps = total_success_transactions / (time.time() - start_experiment_time)
    avg_latency = sum(olap_latencies) / len(olap_latencies) if olap_latencies else 0
    avg_freshness = sum(olap_freshness_history) / len(olap_freshness_history) if olap_freshness_history else 0

    print(f"📈 [THROUGHPUT] Rata-rata TPS      : {final_tps:.2f} Transaksi/detik")
    print(f"⏱️ [LATENCY] Rata-rata Latensi OLAP : {avg_latency:.2f} ms")
    print(f"🍃 [FRESHNESS] Rata-rata Jeda Data  : {avg_freshness:.2f} detik (Target < 1s)")
    print(f"🛑 [ABORT RATE] Persentase Aborted  : {abort_rate:.2f}% (Target mendekati 0%)")
    print("========================================================\n")
    
    # === PROSES GENERATE GRAFIK ASLI (Sesuai Gambar Awal) ===
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # 1. Grafik Kiri: OLTP Data Ingestion (TiKV)
    ax1.plot(oltp_timestamps, oltp_total_rows, color='green', marker='o', linestyle='-')
    ax1.set_title('OLTP: Data Ingestion (TiKV)')
    ax1.set_xlabel('Waktu (detik)')
    ax1.set_ylabel('Total Baris Terinput')
    ax1.grid(True)
    
    # 2. Grafik Kanan: OLAP Stability via TiFlash (Decoupled)
    ax2.plot(olap_timestamps, ax2_latencies := olap_latencies, color='blue', marker='s', linestyle='-')
    ax2.set_title('OLAP: Stability via TiFlash (Decoupled)')
    ax2.set_xlabel('Waktu (detik)')
    ax2.set_ylabel('Latensi (ms)')
    ax2.grid(True)
    
    plt.tight_layout()
    plt.savefig('hasil_eksperimen_htap.png', dpi=300)
    plt.show()