import time
import json
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

KAFKA_TOPIC = "charging_events"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
NUM_STATIONS = 5
TICK_INTERVAL = 2

STATION_IDS = [str(uuid.uuid4()) for _ in range(NUM_STATIONS)]

active_sessions = {}

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_event(session_data, event_type, kwh_delta=0.0, money_total=0.0):
    """Формує словник події для відправки в Kafka"""
    
    details = {
        "voltage": round(random.uniform(220, 240), 1),
        "current": round(random.uniform(10, 32), 1),
        "temp_c": round(random.uniform(20, 45), 1)
    }

    now = datetime.now()

    event = {
        "session_id": session_data['session_id'],
        "station_id": session_data['station_id'],
        "event_time": now.isoformat(),
        "timestamp": now.timestamp(),
        "event_type": event_type,
        "amount_kwh": round(kwh_delta, 4),
        "amount_money": round(money_total, 2),
        "details": json.dumps(details)
    }
    return event

def main():
    producer = get_producer()
    print(f"Producer запущено! Відправка в топік '{KAFKA_TOPIC}'...")
    print(f"Емуляція {NUM_STATIONS} станцій. Натисніть Ctrl+C для зупинки.")

    try:
        while True:
            for session_id in list(active_sessions.keys()):
                session = active_sessions[session_id]
                
                if random.random() < 0.05 and session['kwh_accumulated'] > 0.5:
                    
                    # 1. Подія: SESSION_ENDED
                    end_event = generate_event(session, "SESSION_ENDED")
                    producer.send(KAFKA_TOPIC, end_event)
                    print(f"[{session['station_id'][:8]}] Session Ended: {session_id[:8]}")
                    
                    # 2. Подія: PAYMENT_PROCESSED (Оплата за всю сесію)
                    total_cost = session['kwh_accumulated'] * 10.0 
                    pay_event = generate_event(session, "PAYMENT_PROCESSED", money_total=total_cost)
                    producer.send(KAFKA_TOPIC, pay_event)
                    print(f"[{session['station_id'][:8]}] Payment: {total_cost:.2f} UAH")
                    
                    # Видаляємо з активних
                    del active_sessions[session_id]
                
                else:
                    # --- ПРОЦЕС ЗАРЯДКИ ---
                    kwh_delta = random.uniform(0.1, 0.5)
                    session['kwh_accumulated'] += kwh_delta
                    
                    # Подія: ENERGY_DELIVERED
                    del_event = generate_event(session, "ENERGY_DELIVERED", kwh_delta=kwh_delta)
                    producer.send(KAFKA_TOPIC, del_event)
                    print(f"[{session['station_id'][:8]}] Charging... +{kwh_delta:.3f} kWh")

            active_station_ids = [s['station_id'] for s in active_sessions.values()]
            free_stations = [sid for sid in STATION_IDS if sid not in active_station_ids]

            for station_id in free_stations:
                if random.random() < 0.2:
                    new_session_id = str(uuid.uuid4())
                    
                    active_sessions[new_session_id] = {
                        "session_id": new_session_id,
                        "station_id": station_id,
                        "start_time": time.time(),
                        "kwh_accumulated": 0.0
                    }
                    
                    start_event = generate_event(active_sessions[new_session_id], "SESSION_STARTED")
                    producer.send(KAFKA_TOPIC, start_event)
                    print(f"[{station_id[:8]}] New Session: {new_session_id[:8]}")

            time.sleep(TICK_INTERVAL)

    except KeyboardInterrupt:
        print("\nProducer зупинено користувачем.")
        producer.close()

if __name__ == "__main__":
    main()