"""VM4: consumes topic `orders` into the local file orders_landed.jsonl (Spark Job 1's input).
Stops once no new message has arrived for --idle seconds after the first one.
Usage: python consume_orders.py [--out orders_landed.jsonl] [--idle 20]"""
import argparse
import time

from confluent_kafka import Consumer

BOOTSTRAP = "VM2_IP:9092,VM3_IP:9092"  # <-- the two Kafka brokers
TOPIC = "orders"

ap = argparse.ArgumentParser()
ap.add_argument("--out", default="orders_landed.jsonl")
ap.add_argument("--idle", type=float, default=20)
a = ap.parse_args()

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": f"vm4-orders-{int(time.time())}",  # fresh group each run -> always reads from the start
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
c.subscribe([TOPIC])

n = 0
last = None
with open(a.out, "w", encoding="utf-8") as f:
    while last is None or time.time() - last < a.idle:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("error:", msg.error())
            continue
        f.write(msg.value().decode("utf-8", errors="replace").replace("\n", " ") + "\n")
        n += 1
        last = time.time()
        if n % 10 == 0:
            print(f"{n} records")
c.close()
print(f"DONE: {n} records -> {a.out}")
