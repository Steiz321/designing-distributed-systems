from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from collections import defaultdict

KEYSPACE = 'lab4_energy'
WINDOW_SIZE_SECONDS = 300

def main():
    print("Запуск симуляції Replay (Event Sourcing)...")
    
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect(KEYSPACE)

    print("Зчитування журналу подій (charging_event_log)...")
    rows = session.execute("SELECT station_id, event_time, amount_kwh, amount_money FROM charging_event_log")
    
    replayed_state = defaultdict(lambda: {'kwh': 0.0, 'revenue': 0.0, 'count': 0})

    count = 0
    for row in rows:
        count += 1
        ts = row.event_time.timestamp()
        window_start_ts = ts - (ts % WINDOW_SIZE_SECONDS)
        
        key = (row.station_id, window_start_ts)
        
        replayed_state[key]['kwh'] += row.amount_kwh
        replayed_state[key]['revenue'] += float(row.amount_money if row.amount_money else 0.0)
        replayed_state[key]['count'] += 1

    print(f"Оброблено {count} історичних подій.\n")

    print(f"{'STATION ID':<38} | {'WINDOW START':<20} | {'REPLAYED KWH':<12} | {'REPLAYED REV':<12}")
    print("-" * 90)

    sorted_keys = sorted(replayed_state.keys(), key=lambda x: (x[0], x[1]))

    for station_id, win_start_ts in sorted_keys:
        stats = replayed_state[(station_id, win_start_ts)]
        win_time_str = datetime.fromtimestamp(win_start_ts).strftime('%H:%M:%S')
        
        print(f"{str(station_id):<38} | {win_time_str:<20} | {stats['kwh']:<12.3f} | {stats['revenue']:<12.2f}")

    cluster.shutdown()

if __name__ == "__main__":
    main()