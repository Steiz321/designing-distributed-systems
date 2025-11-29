import time
import statistics
import uuid
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# --- КОНФІГУРАЦІЯ ---
KEYSPACE = 'lab3_ev_network'
ITERATIONS = 50  # Кількість повторів для статистики

def print_stats(name, latencies):
    """Виводить статистику (Avg, p50, p95, p99) у мілісекундах"""
    if not latencies:
        print(f"🔹 {name:<35} | Помилка або немає даних")
        return
        
    # Переводимо в мс
    latencies_ms = [t * 1000 for t in latencies]
    avg = statistics.mean(latencies_ms)
    p50 = statistics.median(latencies_ms)
    p95 = sorted(latencies_ms)[int(len(latencies_ms) * 0.95)]
    p99 = sorted(latencies_ms)[int(len(latencies_ms) * 0.99)]
    
    print(f" {name:<35} | Avg: {avg:6.2f}ms | p50: {p50:6.2f}ms | p95: {p95:6.2f}ms | p99: {p99:6.2f}ms")

def run_benchmark(session, station_id):
    print(f"\n ЗАПУСК BENCHMARK (Iter: {ITERATIONS}, Station: {station_id})")
    print("=" * 100)
    
    # Підготовка дат (середина періоду симуляції)
    now = datetime(2024, 1, 15, 12, 0, 0) 
    six_hours_ago = now - timedelta(hours=6)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Buckets
    hour_bucket = int(now.strftime('%Y%m%d%H'))
    day_bucket = now.date()

    # -------------------------------------------------------------------------
    # QUERY 1: LATEST DATA (LIMIT 100)
    # -------------------------------------------------------------------------
    print("\n--- 1. Query: Latest 100 records ---")
    
    # Schema 1
    stmt = session.prepare("SELECT * FROM charging_events_simple WHERE station_id = ? LIMIT 100")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        session.execute(stmt, [station_id])
        times.append(time.perf_counter() - start)
    print_stats("Schema 1 (Simple)", times)

    # Schema 2
    stmt = session.prepare("SELECT * FROM charging_events_hourly WHERE station_id = ? AND hour_bucket = ? LIMIT 100")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        session.execute(stmt, [station_id, hour_bucket])
        times.append(time.perf_counter() - start)
    print_stats("Schema 2 (Hourly)", times)

    # Schema 3
    stmt = session.prepare("SELECT * FROM charging_sessions_daily WHERE station_id = ? AND day_bucket = ? LIMIT 100")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        session.execute(stmt, [station_id, day_bucket])
        times.append(time.perf_counter() - start)
    print_stats("Schema 3 (Daily)", times)

    # -------------------------------------------------------------------------
    # QUERY 2: TIME RANGE (6 Hours)
    # -------------------------------------------------------------------------
    print("\n--- 2. Query: Time Range (6 Hours) ---")
    
    # Schema 1 (Range query on partition)
    stmt = session.prepare("SELECT * FROM charging_events_simple WHERE station_id = ? AND event_time >= ? AND event_time <= ?")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        list(session.execute(stmt, [station_id, six_hours_ago, now]))
        times.append(time.perf_counter() - start)
    print_stats("Schema 1 (Simple)", times)

    # Schema 2 (Multi-partition query simulation)
    buckets_6h = [int((now - timedelta(hours=h)).strftime('%Y%m%d%H')) for h in range(6)]
    stmt = session.prepare("SELECT * FROM charging_events_hourly WHERE station_id = ? AND hour_bucket = ?")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        for b in buckets_6h:
            list(session.execute(stmt, [station_id, b]))
        times.append(time.perf_counter() - start)
    print_stats("Schema 2 (Hourly - 6 requests)", times)

    # Schema 3 (Single partition range)
    stmt = session.prepare("SELECT * FROM charging_sessions_daily WHERE station_id = ? AND day_bucket = ? AND event_time >= ? AND event_time <= ?")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        list(session.execute(stmt, [station_id, day_bucket, six_hours_ago, now]))
        times.append(time.perf_counter() - start)
    print_stats("Schema 3 (Daily)", times)

    # -------------------------------------------------------------------------
    # QUERY 3: DAILY AGGREGATION (Full Day)
    # -------------------------------------------------------------------------
    print("\n--- 3. Query: Daily Aggregation ---")

    # Schema 1
    stmt = session.prepare("SELECT * FROM charging_events_simple WHERE station_id = ? AND event_time >= ? AND event_time <= ?")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        list(session.execute(stmt, [station_id, start_of_day, end_of_day]))
        times.append(time.perf_counter() - start)
    print_stats("Schema 1 (Simple)", times)

    # Schema 2 (24 requests!)
    buckets_24h = [int((start_of_day + timedelta(hours=h)).strftime('%Y%m%d%H')) for h in range(24)]
    stmt = session.prepare("SELECT * FROM charging_events_hourly WHERE station_id = ? AND hour_bucket = ?")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        for b in buckets_24h:
            list(session.execute(stmt, [station_id, b]))
        times.append(time.perf_counter() - start)
    print_stats("Schema 2 (Hourly - 24 requests)", times)

    # Schema 3 (1 partition)
    stmt = session.prepare("SELECT * FROM charging_sessions_daily WHERE station_id = ? AND day_bucket = ?")
    times = []
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        list(session.execute(stmt, [station_id, day_bucket]))
        times.append(time.perf_counter() - start)
    print_stats("Schema 3 (Daily)", times)

    # -------------------------------------------------------------------------
    # QUERY 4: FILTERING (ALLOW FILTERING only)
    # -------------------------------------------------------------------------
    print("\n--- 4. Query: Filtering (ALLOW FILTERING) ---")

    # Використовуємо Simple схему для найчеснішого тесту "поганої" практики
    # Шукаємо 'Type 2' конектори
    
    # query_string = f"SELECT * FROM charging_events_simple WHERE station_id = {station_id} AND connector_type = 'Type 2' ALLOW FILTERING"
    # Для prepared statement з ALLOW FILTERING треба бути обережним, 
    # тому використовуємо прямий рядок для простоти тесту антипатерну.
    
    query = SimpleStatement(f"SELECT * FROM charging_events_simple WHERE station_id = {station_id.urn[9:]} AND connector_type = 'Type 2' ALLOW FILTERING")
    
    times = []
    for _ in range(5):  # Зменшили кількість ітерацій
        start = time.perf_counter()
        list(session.execute(query))
        times.append(time.perf_counter() - start)
    print_stats("ALLOW FILTERING (Schema 1)", times)

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect(KEYSPACE)
    
    # Отримуємо валідний ID
    row = session.execute("SELECT station_id FROM charging_events_simple LIMIT 1").one()
    if not row:
        print("Немає даних у таблиці charging_events_simple! Запустіть генерацію (Етап 2).")
        return

    run_benchmark(session, row.station_id)
    cluster.shutdown()

if __name__ == "__main__":
    main()