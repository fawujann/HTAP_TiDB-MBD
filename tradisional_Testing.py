import pymysql
import pandas as pd
import threading
import time
import sys
import matplotlib.pyplot as plt

# === CONFIGURATION (Gunakan data cluster TiDB kamu) ===
DB_CONFIG = {
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'port': 4000,
    'user': '2qEWFvfev7wavRf.root',
    'password': 'NIbLpVSJekvn2Sb6',
    'database': 'smart_farming_db',
    'autocommit': True,
    'ssl': {'ssl_disabled': False}
}

stop_experiment = False
start_experiment_time = None

def connect_db():
    return pymysql.connect(**DB_CONFIG)

# === STRUKTUR DATA UNTUK METRIK & VISUALISASI ===
olap_timestamps = []
olap_latencies = []

total_success_transactions = 0
total_failed_transactions = 0
olap_freshness_history = []

# === 1. PROSES TRANSAKSI (OLTP) ===
def oltp_worker():
    global total_success_transactions, total_failed_transactions
    conn = connect_db()
    cursor = conn.cursor()
    print("🚀 [TRADISIONAL] Memulai Ingest Data Sensor ke TiKV...")
    try:
        df = pd.read_csv('crop_recommendation.csv')
        batch_size = 50
        while not stop_experiment:
            for i in range(0, len(df), batch_size):
                if stop_experiment: break
                batch = df.iloc[i : i + batch_size]
                data = [tuple(x) for x in batch[['N', 'P', 'K', 'temperature', 'humidity', 'ph', 'rainfall', 'label']].values]
                
                sql = "INSERT INTO sensor_iot (N, P, K, temperature, humidity, ph, rainfall, label) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                
                try:
                    cursor.executemany(sql, data)
                    total_success_transactions += 1
                except (pymysql.OperationalError, pymysql.InternalError):
                    # Pada arsitektur tradisional, resource contention rawan menaikkan abort rate
                    total_failed_transactions += 1
                
                time.sleep(1) # Dipercepat ke 1 detik agar beban kontensi lebih terasa
    finally:
        conn.close()

# === 2. PROSES ANALITIK TRADISIONAL (DIPAKSA KE TIKV) ===
def olap_traditional_worker():
    global start_experiment_time
    conn = connect_db()
    cursor = conn.cursor()
    print("⚠️ [TRADISIONAL] Memulai Analitik Berulang di Jalur TiKV (Row Store)...")
    try:
        while not stop_experiment:
            start_time = time.time()
            
            # KOREKSI: Kolom diselaraskan dengan kueri uji coba (`item` dan `hg/ha_yield`)
            sql = """SELECT /*+ READ_FROM_STORAGE(tikv[yield_history]) */ 
                     `item`, AVG(`hg/ha_yield`) as avg_yield, AVG(avg_temp) as temp
                     FROM yield_history GROUP BY `item`;"""
            
            cursor.execute(sql)
            cursor.fetchall()
            
            latency = (time.time() - start_time) * 1000
            current_time = time.time() - start_experiment_time
            
            # Mengukur Data Freshness pada arsitektur tradisional (TiKV)
            sql_freshness = """SELECT /*+ READ_FROM_STORAGE(tikv[sensor_iot]) */ 
                               TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) as freshness_seconds 
                               FROM sensor_iot;"""
            try:
                cursor.execute(sql_freshness)
                freshness_result = cursor.fetchone()
                data_freshness = float(freshness_result[0]) if freshness_result and freshness_result[0] is not None else 0.0
            except Exception:
                data_freshness = 0.0

            # Catat data riwayat untuk grafik dan metrik teks
            olap_timestamps.append(current_time)
            olap_latencies.append(latency)
            olap_freshness_history.append(data_freshness)
            
            print(f"⚠️ [BASELINE] Latency TiKV: {latency:.2f}ms (Beban pada Transaksi)")
            
            # Tanpa jeda/sleep agar beban kontensinya maksimal (Stress Test)
    finally:
        conn.close()

if __name__ == "__main__":
    start_experiment_time = time.time()
    
    t1 = threading.Thread(target=oltp_worker)
    t2 = threading.Thread(target=olap_traditional_worker)
    
    t1.start()
    t2.start()
    
    try:
        time.sleep(60) # Jalankan simulasi selama 1 menit sesuai spesifikasi awal
    except KeyboardInterrupt:
        pass
    
    stop_experiment = True
    t1.join()
    t2.join()
    
    # === OUTPUT AKHIR METRIK EVALUASI TRADISIONAL (Teks di Konsol) ===
    print("\n================ EVALUASI METRIK SISTEM (TRADISIONAL) ================")
    total_transactions = total_success_transactions + total_failed_transactions
    abort_rate = (total_failed_transactions / total_transactions) * 100 if total_transactions > 0 else 0
    final_tps = total_success_transactions / (time.time() - start_experiment_time)
    
    avg_latency = sum(olap_latencies) / len(olap_latencies) if olap_latencies else 0
    min_latency = min(olap_latencies) if olap_latencies else 0
    max_latency = max(olap_latencies) if olap_latencies else 0
    peak_time = olap_timestamps[olap_latencies.index(max_latency)] if olap_latencies else 0
    avg_freshness = sum(olap_freshness_history) / len(olap_freshness_history) if olap_freshness_history else 0

    print(f"📈 [THROUGHPUT] Rata-rata TPS      : {final_tps:.2f} Transaksi/detik")
    print(f"⏱  [LATENCY] Rata-rata Latensi OLAP : {avg_latency:.2f} ms")
    print(f"🍃 [FRESHNESS] Rata-rata Jeda Data  : {avg_freshness:.2f} detik")
    print(f"🛑 [ABORT RATE] Persentase Aborted  : {abort_rate:.2f}%")
    print("======================================================================\n")
    
    # === PROSES GENERATE GRAFIK BASELINE (Sesuai Persis dengan Screenshot) ===
    plt.figure(figsize=(12, 7))
    
    # Plot utama (Line chart dengan marker bulat berongga warna tomato)
    plt.plot(olap_timestamps, olap_latencies, color='tomato', marker='o', markerfacecolor='white', 
             markeredgecolor='tomato', markersize=5, linestyle='-', label='TiKV Latency (Row Store)')
    
    # Menambahkan garis horizontal rata-rata (Dashed line biru tua)
    plt.axhline(y=avg_latency, color='#2c3e50', linestyle='--', linewidth=2, 
                label=f'Rata-rata: {avg_latency:.2f}ms')
    
    # Judul dan Label Sumbu
    plt.title('Analisis Performa Baseline: Jalur Transaksi (TiKV)\n(Resource Contention: OLTP + OLAP)', 
              fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Waktu Simulasi (detik)', fontsize=12)
    plt.ylabel('Latensi Query (ms)', fontsize=12)
    
    # Kotak Ringkasan Performa (Kiri Atas)
    summary_text = f"Ringkasan Performa:\nMin: {min_latency:.2f}ms\nAvg: {avg_latency:.2f}ms\nMax: {max_latency:.2f}ms"
    bbox_props = dict(boxstyle="round,pad=0.5", fc="white", ec="black", lw=1, alpha=0.8)
    plt.gca().text(0.02, 0.95, summary_text, transform=plt.gca().transAxes, fontsize=11,
            verticalalignment='top', bbox=bbox_props)
    
    # Anotasi Titik Tertinggi (Peak Latency dengan Panah)
    plt.annotate(f'Peak: {max_latency:.2f}ms', 
                 xy=(peak_time, max_latency), 
                 xytext=(peak_time + 5, max_latency + 5),
                 fontweight='bold', color='crimson', fontsize=11,
                 arrowprops=dict(facecolor='black', shrink=0.08, width=1, headwidth=6))
    
    # Grid dan Legend Styling
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend(loc='upper right', shadow=True, frameon=True, facecolor='white')
    
    # Batas Sumbu Agar Rapi
    if olap_latencies:
        plt.ylim(bottom=min_latency - 5, top=max_latency + 15)
        
    plt.tight_layout()
    plt.savefig('hasil_eksperimen_tradisional.png', dpi=300)
    plt.show()
    print("\n✅ Simulasi Tradisional Selesai. Grafik disimpan sebagai 'hasil_eksperimen_tradisional.png'")