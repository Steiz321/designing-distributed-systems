import uuid
import random
import time
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

# --- КОНФІГУРАЦІЯ ---
KEYSPACE = 'lab3_ev_network'
NUM_STATIONS = 50          # Кількість станцій
DAYS_TO_SIMULATE = 30      # Кількість днів
READINGS_PER_HOUR = 30     # Частота записів (наприклад, кожні 2 хвилини)
BATCH_SIZE = 1000          # Розмір пакету для відправки в драйвер

# Розрахунок очікуваної кількості
EXPECTED_ROWS = NUM_STATIONS * DAYS_TO_SIMULATE * 24 * READINGS_PER_HOUR
print(f"План генерації: {EXPECTED_ROWS:,} рядків.")

def get_power_value(hour):
    """
    Логіка генерації потужності відповідно до завдання:
    - Денний пік (12:00–14:00): 3–5 кВт
    - Ранок/вечір (6–9, 17–19): 1–3 кВт
    - Ніч (21–5): 0–0.2 кВт
    - Інший час: середнє значення (0.5-2 кВт)
    """
    val = 0.0
    if 12 <= hour < 14:
        val = random.uniform(3.0, 5.0)
    elif (6 <= hour < 9) or (17 <= hour < 19):
        val = random.uniform(1.0, 3.0)
    elif (21 <= hour) or (hour < 5):
        val = random.uniform(0.0, 0.2)
    else:
        val = random.uniform(0.5, 2.0)
    
    # Імітація "Хмарності" (випадкові падіння на 30-70%)
    # У контексті EV це може бути просідання напруги в мережі
    if random.random() < 0.2: # 20% шанс просідання
        drop_factor = random.uniform(0.3, 0.7)
        val *= drop_factor
        
    return round(val, 2)

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    
    # Підготовка Keyspace
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};
    """)
    session.set_keyspace(KEYSPACE)

    # Підготовка запиту (PreparedStatement - це критично для швидкості)
    # Вставляємо в Schema 2 (Hourly Bucketing)
    insert_stmt = session.prepare("""
        INSERT INTO charging_events_hourly 
        (station_id, hour_bucket, event_time, connector_type, power_kw, session_duration)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    print("Генерація даних у пам'яті та підготовка до запису...")
    
    # Генеруємо ID станцій
    station_ids = [uuid.uuid4() for _ in range(NUM_STATIONS)]
    connector_types = ['Type 2', 'CCS 2', 'CHAdeMO', 'Tesla Supercharger']
    
    start_date = datetime(2024, 1, 1)
    
    all_params = []
    
    # --- ГЕНЕРАЦІЯ ДАНИХ ---
    start_gen_time = time.time()
    
    for day in range(DAYS_TO_SIMULATE):
        current_day = start_date + timedelta(days=day)
        
        for hour in range(24):
            # Формуємо bucket_hour (YYYYMMDDHH)
            bucket_val = int(current_day.strftime('%Y%m%d') + f"{hour:02d}")
            
            for _ in range(READINGS_PER_HOUR):
                # Випадкова хвилина/секунда всередині години
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                event_time = current_day.replace(hour=hour, minute=minute, second=second)
                
                for station in station_ids:
                    power = get_power_value(hour)
                    conn = random.choice(connector_types)
                    duration = 0 # Телеметрія миттєва, тривалість тут 0
                    
                    # Додаємо кортеж параметрів до списку
                    all_params.append((station, bucket_val, event_time, conn, power, duration))
    
    gen_duration = time.time() - start_gen_time
    print(f"Дані згенеровано у пам'яті за {gen_duration:.2f} с. Кількість: {len(all_params)}")

    # --- ЗАПИС У CASSANDRA (Concurrent) ---
    print(f"Початок запису в Cassandra...")
    start_db_time = time.time()
    
    # Розбиваємо на чанки для контролю пам'яті (хоча execute_concurrent розумний)
    total_records = len(all_params)
    
    # Використовуємо execute_concurrent_with_args для масової паралельної вставки
    # concurrency=100 означає 100 паралельних запитів до бази
    for i in range(0, total_records, BATCH_SIZE):
        batch = all_params[i : i + BATCH_SIZE]
        results = execute_concurrent_with_args(session, insert_stmt, batch, concurrency=100)
        
        # Вивід прогресу
        if i % 50000 == 0:
             print(f"   Записано {i}/{total_records} рядків...")
             
    end_db_time = time.time()
    total_time = end_db_time - start_db_time
    
    print("\n" + "="*40)
    print("ЗАВЕРШЕНО!")
    print(f"Всього записів: {total_records}")
    print(f"Час запису в БД: {total_time:.2f} с ({total_time/60:.2f} хв)")
    print(f"Швидкість: {total_records/total_time:.0f} записів/сек")
    print("="*40)
    
    cluster.shutdown()

if __name__ == "__main__":
    main()