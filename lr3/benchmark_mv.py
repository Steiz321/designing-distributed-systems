import time
import statistics
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# --- КОНФІГУРАЦІЯ ---
KEYSPACE = 'lab3_ev_network'
ITERATIONS = 20  # Менше ітерацій, бо запит без MV повільний

def print_comparison(name, time_no_mv, time_with_mv):
    avg_no_mv = statistics.mean(time_no_mv) * 1000
    avg_with_mv = statistics.mean(time_with_mv) * 1000
    
    speedup = avg_no_mv / avg_with_mv if avg_with_mv > 0 else 0
    
    print(f"\nРЕЗУЛЬТАТИ: {name}")
    print("-" * 50)
    print(f"Без MV (ALLOW FILTERING): {avg_no_mv:8.2f} ms")
    print(f"З MV (Direct Access):     {avg_with_mv:8.2f} ms")
    print("-" * 50)
    print(f"ПРИСКОРЕННЯ (Speedup):    {speedup:8.2f}x")

def run_mv_benchmark(session):
    print("Отримання тестових даних (Station ID)...")
    # Беремо реальну станцію і реальний bucket
    row = session.execute("SELECT station_id, hour_bucket FROM charging_events_hourly LIMIT 1").one()
    if not row:
        print("Немає даних для тесту.")
        return
        
    st_id = row.station_id
    h_bucket = row.hour_bucket
    
    print(f"Тестуємо на Station: {st_id}, Bucket: {h_bucket}")

    # =========================================================================
    # ТЕСТ 1: High Power (> 2.5 kW)
    # =========================================================================
    print("\nТест 1: Пошук записів з потужністю > 2.5 кВт")
    
    # А. Повільний запит (Base Table)
    # Ми змушені читати все і фільтрувати на льоту
    query_bad = SimpleStatement(
        f"SELECT * FROM charging_events_hourly WHERE station_id={st_id.urn[9:]} AND hour_bucket={h_bucket} AND power_kw > 2.5 ALLOW FILTERING"
    )
    
    # Б. Швидкий запит (Materialized View)
    # power_kw є частиною ключа, тому ми шукаємо діапазон, а не фільтруємо
    query_good = SimpleStatement(
        f"SELECT * FROM events_high_power WHERE station_id={st_id.urn[9:]} AND hour_bucket={h_bucket} AND power_kw > 2.5"
    )

    times_bad = []
    times_good = []

    print("   Running 'Bad' query...", end='', flush=True)
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        list(session.execute(query_bad))
        times_bad.append(time.perf_counter() - start)
    print(" Done.")

    print("   Running 'Good' query...", end='', flush=True)
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        list(session.execute(query_good))
        times_good.append(time.perf_counter() - start)
    print(" Done.")
    
    print_comparison("High Power Query (> 2.5 kW)", times_bad, times_good)

    # =========================================================================
    # ТЕСТ 2: Low Power (< 1.0 kW)
    # =========================================================================
    print("\nТест 2: Пошук записів з потужністю < 1.0 кВт")
    
    query_bad = SimpleStatement(
        f"SELECT * FROM charging_events_hourly WHERE station_id={st_id.urn[9:]} AND hour_bucket={h_bucket} AND power_kw < 1.0 ALLOW FILTERING"
    )
    
    query_good = SimpleStatement(
        f"SELECT * FROM events_low_power WHERE station_id={st_id.urn[9:]} AND hour_bucket={h_bucket} AND power_kw < 1.0"
    )

    times_bad = []
    times_good = []

    print("   Running 'Bad' query...", end='', flush=True)
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        list(session.execute(query_bad))
        times_bad.append(time.perf_counter() - start)
    print(" Done.")

    print("   Running 'Good' query...", end='', flush=True)
    for _ in range(ITERATIONS):
        start = time.perf_counter()
        list(session.execute(query_good))
        times_good.append(time.perf_counter() - start)
    print(" Done.")
    
    print_comparison("Low Power Query (< 1.0 kW)", times_bad, times_good)

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect(KEYSPACE)
    run_mv_benchmark(session)
    cluster.shutdown()

if __name__ == "__main__":
    main()