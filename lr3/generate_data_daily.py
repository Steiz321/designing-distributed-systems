import uuid
import random
import time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

# --- КОНФІГУРАЦІЯ ---
KEYSPACE = 'lab3_ev_network'
NUM_STATIONS = 50          
DAYS_TO_SIMULATE = 30      
READINGS_PER_HOUR = 30     
BATCH_SIZE = 1000          

def get_power_value(hour):
    val = 0.0
    if 12 <= hour < 14: val = random.uniform(3.0, 5.0)
    elif (6 <= hour < 9) or (17 <= hour < 19): val = random.uniform(1.0, 3.0)
    elif (21 <= hour) or (hour < 5): val = random.uniform(0.0, 0.2)
    else: val = random.uniform(0.5, 2.0)
    if random.random() < 0.2: val *= random.uniform(0.3, 0.7)
    return round(val, 2)

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect(KEYSPACE)
    
    # Запит з day_bucket (тип date)
    insert_stmt = session.prepare("""
        INSERT INTO charging_sessions_daily 
        (station_id, day_bucket, event_time, connector_type, power_kw, session_duration)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    print("(Daily Schema) Генерація даних у пам'яті...")
    
    station_ids = [uuid.uuid4() for _ in range(NUM_STATIONS)]
    connector_types = ['Type 2', 'CCS 2', 'CHAdeMO', 'Tesla Supercharger']
    start_date = datetime(2024, 1, 1)
    all_params = []
    
    for day in range(DAYS_TO_SIMULATE):
        current_day = start_date + timedelta(days=day)
        # Формуємо об'єкт дати для Cassandra
        day_bucket_val = current_day.date() 
        
        for hour in range(24):
            for _ in range(READINGS_PER_HOUR):
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                event_time = current_day.replace(hour=hour, minute=minute, second=second)
                
                for station in station_ids:
                    power = get_power_value(hour)
                    conn = random.choice(connector_types)
                    duration = 0 
                    
                    # Передаємо day_bucket_val
                    all_params.append((station, day_bucket_val, event_time, conn, power, duration))
    
    print(f"Згенеровано {len(all_params)} рядків.")

    print(f"Початок запису в таблицю charging_sessions_daily...")
    start_db_time = time.time()
    
    total_records = len(all_params)
    for i in range(0, total_records, BATCH_SIZE):
        batch = all_params[i : i + BATCH_SIZE]
        execute_concurrent_with_args(session, insert_stmt, batch, concurrency=100)
        if i % 50000 == 0: print(f"   Записано {i}/{total_records}...")
             
    total_time = time.time() - start_db_time
    print("ЗАВЕРШЕНО!")
    print(f"Всього записів: {total_records}")
    print(f"Час запису в БД: {total_time:.2f} с ({total_time/60:.2f} хв)")
    print(f"Швидкість: {total_records/total_time:.0f} записів/сек")
    print("="*40)
    cluster.shutdown()

if __name__ == "__main__":
    main()