"""Replays a CSV into Kafka in event-time order.
L1: python producer.py --csv data/transactions.csv --start-at 14:30:00"""
import argparse
import csv
import json
import time
from datetime import datetime

from confluent_kafka import Producer

BOOTSTRAP = "10.147.17.12:9092,10.147.17.13:9092"  # <-- ZeroTier IPs of L2, L3 VMs (the Kafka brokers)
TOPIC = "transactions"
DATA_START = datetime(2026, 10, 5, 10, 0, 0)  # matches the CSV's event_time column

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--speedup", type=float, default=1.0)
ap.add_argument("--start-at", help="wall-clock HH:MM:SS -- gives L2/L3/L4 time to be up before the replay begins")
a = ap.parse_args()

rows = list(csv.DictReader(open(a.csv)))
assert all(x["event_time"] <= y["event_time"] for x, y in zip(rows, rows[1:])), "CSV not sorted by event_time"

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 5,
})
acked = 0


def on_delivery(err, msg):
    global acked
    if err:
        print(f"{time.strftime('%H:%M:%S')} FAILED {msg.key().decode()}: {err}")
        return
    acked += 1
    v = json.loads(msg.value())
    print(f"{time.strftime('%H:%M:%S')} SENT {v['txn_id']} card={v['card_id']} "
          f"event={v['event_time'][11:19]} -> p{msg.partition()} off {msg.offset()}")


if a.start_at:
    h, m, s = map(int, a.start_at.split(":"))
    target = datetime.now().replace(hour=h, minute=m, second=s, microsecond=0)
    print(f"waiting until {a.start_at} ...")
    time.sleep(max(0.0, (target - datetime.now()).total_seconds()))

start_wall = time.time()
last_report = start_wall
for r in rows:
    offset = (datetime.fromisoformat(r["event_time"]) - DATA_START).total_seconds() / a.speedup
    while (wait := offset - (time.time() - start_wall)) > 0:
        p.poll(min(wait, 0.2))
    msg = {"txn_id": r["txn_id"], "card_id": int(r["card_id"]), "amount": float(r["amount"]),
           "merchant": r["merchant"], "city": r["city"], "lat": float(r["lat"]), "lon": float(r["lon"]),
           "event_time": r["event_time"]}
    while True:
        try:
            p.produce(TOPIC, key=str(msg["card_id"]), value=json.dumps(msg), on_delivery=on_delivery)
            break
        except BufferError:
            p.poll(0.5)
    p.poll(0)
    if time.time() - last_report >= 10:
        print(f"--- progress: {acked}/{len(rows)} acked")
        last_report = time.time()

p.flush()
print(f"DONE: {acked}/{len(rows)} acked")
