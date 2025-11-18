import uuid
import random
from datetime import datetime, timedelta, date
from decimal import Decimal
from cassandra.cluster import Cluster

KEYSPACE = "ev_charging_network"
STATION_COUNT = 20 
PORTS_PER_STATION = 4 
N_SESSIONS = 500
N_USERS = 100
SIMULATION_DATE = date(2025, 11, 14)

def create_schema(session):
    """
    Створює Keyspace та всі необхідні таблиці, якщо вони не існують.
    """
    print(f"Створення keyspace '{KEYSPACE}' (якщо не існує)...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{ 
            'class' : 'SimpleStrategy', 
            'replication_factor' : 1 
        }};
    """)
    
    print(f"Використання keyspace '{KEYSPACE}'...")
    session.set_keyspace(KEYSPACE)

    print("Створення таблиць (якщо не існують)...")
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS port_status (
            station_id uuid,
            port_id int,
            status text,
            power_kw float,
            current_session_start timestamp,
            PRIMARY KEY (station_id, port_id)
        );
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            user_id uuid,
            start_time timestamp,
            end_time timestamp,
            station_id uuid,
            port_id int,
            energy_consumed_kwh float,
            session_cost decimal,
            PRIMARY KEY (user_id, start_time)
        ) WITH CLUSTERING ORDER BY (start_time DESC);
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS station_daily_summary (
            station_id uuid,
            summary_date date,
            total_sessions int,
            total_energy_kwh float,
            total_revenue decimal,
            avg_session_duration_min int,
            PRIMARY KEY (station_id, summary_date)
        ) WITH CLUSTERING ORDER BY (summary_date DESC);
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS station_hourly_analytics (
            station_id uuid,
            summary_date date,
            hour_of_day int,
            session_count int,
            avg_power_kw float,
            peak_power_kw float,
            PRIMARY KEY ((station_id, summary_date), hour_of_day)
        );
    """)
    print("Схему успішно створено/перевірено.")

def generate_and_insert_data(session):
    """
    Генерує та вставляє тестові дані.
    Повертає ID станції та ID користувача для подальшого аналізу.
    """
    print("\nПочаток генерації та вставки даних...")

    station_ids = [uuid.uuid4() for _ in range(STATION_COUNT)]
    user_ids = [uuid.uuid4() for _ in range(N_USERS)]

    insert_status_stmt = session.prepare(
        "INSERT INTO port_status (station_id, port_id, status, power_kw, current_session_start) VALUES (?, ?, ?, ?, ?)"
    )
    insert_session_stmt = session.prepare(
        "INSERT INTO user_sessions (user_id, start_time, end_time, station_id, port_id, energy_consumed_kwh, session_cost) VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    insert_daily_stmt = session.prepare(
        "INSERT INTO station_daily_summary (station_id, summary_date, total_sessions, total_energy_kwh, total_revenue, avg_session_duration_min) VALUES (?, ?, ?, ?, ?, ?)"
    )
    insert_hourly_stmt = session.prepare(
        "INSERT INTO station_hourly_analytics (station_id, summary_date, hour_of_day, session_count, avg_power_kw, peak_power_kw) VALUES (?, ?, ?, ?, ?, ?)"
    )

    print(f"Вставка даних у 'port_status' ({STATION_COUNT * PORTS_PER_STATION} записів)...")
    for station_id in station_ids:
        for port_id in range(1, PORTS_PER_STATION + 1):
            status = random.choice(['available', 'charging', 'offline'])
            start_time = None
            power = 0.0
            if status == 'charging':
                start_time = datetime.now() - timedelta(minutes=random.randint(5, 45))
                power = random.choice([11.0, 22.0, 50.0])
            session.execute(insert_status_stmt, [station_id, port_id, status, power, start_time])

    print(f"Вставка даних у 'user_sessions' ({N_SESSIONS} записів)...")
    for _ in range(N_SESSIONS):
        start_hour = random.randint(0, 23)
        start_minute = random.randint(0, 59)
        start_time = datetime(SIMULATION_DATE.year, SIMULATION_DATE.month, SIMULATION_DATE.day, start_hour, start_minute)
        duration_min = random.randint(20, 180)
        end_time = start_time + timedelta(minutes=duration_min)
        energy = (duration_min / 60) * random.uniform(7.0, 22.0)
        cost = Decimal(str(energy * random.uniform(0.45, 0.55)))
        session.execute(insert_session_stmt, [
            random.choice(user_ids), start_time, end_time,
            random.choice(station_ids), random.randint(1, PORTS_PER_STATION),
            round(energy, 2), cost.quantize(Decimal('0.01'))
        ])

    print(f"Вставка даних у 'station_daily_summary' ({STATION_COUNT} записів)...")
    for station_id in station_ids:
        total_sess = random.randint(15, 70)
        total_nrg = total_sess * random.uniform(10.0, 15.0)
        total_rev = Decimal(str(total_nrg * 0.5))
        session.execute(insert_daily_stmt, [
            station_id, SIMULATION_DATE, total_sess,
            round(total_nrg, 2), total_rev.quantize(Decimal('0.01')),
            random.randint(30, 90)
        ])

    print(f"Вставка даних у 'station_hourly_analytics' ({STATION_COUNT * 24} записів)...")
    for station_id in station_ids:
        for hour in range(24):
            if 6 <= hour <= 22:
                count, avg_pwr, peak_pwr = random.randint(2, 8), random.uniform(11.0, 22.0), random.uniform(22.0, 50.0)
            else:
                count = random.randint(0, 2)
                avg_pwr = random.uniform(7.0, 11.0) if count > 0 else 0.0
                peak_pwr = random.uniform(avg_pwr, 22.0) if count > 0 else 0.0
            session.execute(insert_hourly_stmt, [
                station_id, SIMULATION_DATE, hour,
                count, round(avg_pwr, 2), round(peak_pwr, 2)
            ])
            
    print("Всі дані успішно згенеровано та вставлено.")
    
    return random.choice(station_ids), random.choice(user_ids)

def perform_analysis(session, station_id_to_check, user_id_to_check):
    """
    Виконує базовий аналіз даних та виводить результати в консоль.
    """
    print("\n--- Початок аналізу даних ---")

    try:
        count_result = session.execute("SELECT COUNT(*) FROM user_sessions").one()
        print(f"\n1. Загальна кількість записів:")
        print(f"   - Всього згенеровано сесій: {count_result.count}")
    except Exception as e:
        print(f"Помилка при підрахунку COUNT(*): {e}")

    try:
        avg_result = session.execute(
            "SELECT AVG(total_sessions), AVG(total_energy_kwh), AVG(total_revenue) FROM station_daily_summary"
        ).one()
        
        print(f"\n2. Середні показники (на станцію за добу):")
        print(f"   - Середня кількість сесій: {avg_result.system_avg_total_sessions:.1f}")
        print(f"   - Середнє споживання енергії: {avg_result.system_avg_total_energy_kwh:.2f} кВт*год")
        print(f"   - Середній дохід: {avg_result.system_avg_total_revenue:.2f} у.о.")
    except Exception as e:
        print(f"Помилка при розрахунку AVG: {e}")

    try:
        print(f"\n3. Приклад оперативних даних (стан портів станції {station_id_to_check}):")
        
        port_rows = session.execute(
            "SELECT port_id, status, power_kw FROM port_status WHERE station_id = %s", 
            (station_id_to_check,)
        )
        
        for row in port_rows:
            print(f"   - Порт {row.port_id}: {row.status.upper()} (Потужність: {row.power_kw} кВт)")
    except Exception as e:
        print(f"Помилка при запиті port_status: {e}")

    try:
        print(f"\n4. Приклад історії сесій (останні 5 для користувача {user_id_to_check}):")
        
        session_rows = session.execute(
            "SELECT start_time, energy_consumed_kwh, session_cost FROM user_sessions WHERE user_id = %s LIMIT 5",
            (user_id_to_check,)
        )
        
        i = 0
        for row in session_rows:
            i += 1
            print(f"   - {row.start_time} | Спожито: {row.energy_consumed_kwh} кВт*год | Вартість: {row.session_cost} у.о.")
        
        if i == 0:
            print(f"   - Для цього користувача не знайдено сесій (можливо, ID не збігся).")
            
    except Exception as e:
        print(f"Помилка при запиті user_sessions: {e}")

    print("\n--- Аналіз завершено ---")

def main():
    cluster = None
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        print("Успішно підключено до кластера Cassandra.")

        create_schema(session)

        print("\nПеревірка наявності даних...")
        
        count_row = session.execute("SELECT COUNT(*) FROM user_sessions").one()
        
        if count_row.count == 0:
            print("База даних порожня. Запускаємо генерацію даних...")
            analysis_station_id, analysis_user_id = generate_and_insert_data(session)
        else:
            print(f"Дані вже існують ({count_row.count} сесій). Пропускаємо генерацію.")
            print("Отримання ID з існуючих даних для аналізу...")
            
            try:
                station_row = session.execute("SELECT station_id FROM port_status LIMIT 1").one()
                user_row = session.execute("SELECT user_id FROM user_sessions LIMIT 1").one()
                
                analysis_station_id = station_row.station_id
                analysis_user_id = user_row.user_id
                print(f"   - Використовуємо Station ID: {analysis_station_id}")
                print(f"   - Використовуємо User ID: {analysis_user_id}")
            except Exception as e:
                print(f"Помилка отримання існуючих ID: {e}. Зупиняємо скрипт.")
                return

        perform_analysis(session, analysis_station_id, analysis_user_id)

    except Exception as e:
        print(f"Сталася головна помилка: {e}")
        print("Будь ласка, переконайтеся, що Cassandra запущена і 'cassandra-driver' встановлено.")
    finally:
        if cluster:
            cluster.shutdown()
            print("\nЗ'єднання з кластером закрито.")

if __name__ == "__main__":
    main()