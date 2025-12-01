import os
import faust
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KAFKA_BROKER = 'kafka://localhost:9092'
KEYSPACE = 'lab4_energy'
WINDOW_SIZE = 300

print("Підключення до Cassandra...")
cluster = Cluster(['127.0.0.1'])
session = cluster.connect(KEYSPACE)

insert_log_stmt = session.prepare("""
    INSERT INTO charging_event_log 
    (session_id, event_time, event_type, station_id, amount_kwh, amount_money, details)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""")

insert_agg_stmt = session.prepare("""
    INSERT INTO station_utilization_state
    (station_id, window_start, window_end, total_energy_kwh, total_revenue, active_sessions_count)
    VALUES (?, ?, ?, ?, ?, ?)
""")
print("Cassandra підключена.")

class ChargingEvent(faust.Record):
    session_id: str
    station_id: str
    event_time: str
    event_type: str 
    timestamp: float = 0.0 
    amount_kwh: float = 0.0
    amount_money: float = 0.0
    details: str = ""

class StationStats(faust.Record):
    total_kwh: float = 0.0
    total_revenue: float = 0.0
    count: int = 0

app = faust.App(
    'energy-stream-v1',
    broker=KAFKA_BROKER,
    processing_guarantee='exactly_once',
    topic_partitions=1,
)

topic = app.topic('charging_events', value_type=ChargingEvent)

stats_table = app.Table(
    'station_stats',
    default=StationStats,
).tumbling(WINDOW_SIZE).relative_to_field(ChargingEvent.timestamp)

@app.agent(topic)
async def process_charging(events):
    async for event in events.group_by(ChargingEvent.station_id):
        
        dt_object = datetime.fromisoformat(event.event_time)
        
        session.execute(insert_log_stmt, [
            custom_uuid(event.session_id),
            dt_object,
            event.event_type,
            custom_uuid(event.station_id),
            event.amount_kwh,
            event.amount_money,
            event.details
        ])

        current_stats = stats_table[event.station_id].current()
        
        current_stats.total_kwh += event.amount_kwh
        current_stats.total_revenue += event.amount_money
        current_stats.count += 1
        
        stats_table[event.station_id] = current_stats

        timestamp = dt_object.timestamp()
        window_start_ts = timestamp - (timestamp % WINDOW_SIZE)
        window_end_ts = window_start_ts + WINDOW_SIZE
        
        w_start = datetime.fromtimestamp(window_start_ts)
        w_end = datetime.fromtimestamp(window_end_ts)

        session.execute(insert_agg_stmt, [
            custom_uuid(event.station_id),
            w_start,
            w_end,
            round(current_stats.total_kwh, 4),
            round(current_stats.total_revenue, 2),
            current_stats.count
        ])
        
        print(f"Processed: {event.event_type} | Station: {event.station_id[:8]} | Window: {w_start.time()}")

def custom_uuid(s):
    from uuid import UUID
    return UUID(s)

if __name__ == '__main__':
    app.main()