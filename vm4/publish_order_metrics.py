"""VM4: publishes every record of order_metrics.jsonl (Spark Job 1 output) to Kafka topic `order_metrics`.
Usage: python publish_order_metrics.py [--file order_metrics.jsonl]"""
import argparse
import json
import time

from confluent_kafka import Producer

BOOTSTRAP = "172.22.134.139:9092,172.22.159.246:9092"  # <-- the two Kafka brokers
TOPIC = "order_metrics"

ap = argparse.ArgumentParser()
ap.add_argument("--file", default="order_metrics.jsonl")
a = ap.parse_args()

p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True, "linger.ms": 5})
acked = 0


def on_delivery(err, msg):
    global acked
    if err:
        print(f"{time.strftime('%H:%M:%S')} FAILED: {err}")
        return
    acked += 1


lines = [l.strip() for l in open(a.file, encoding="utf-8") if l.strip()]
for line in lines:
    key = str(json.loads(line)["order_id"])
    while True:
        try:
            p.produce(TOPIC, key=key, value=line.encode("utf-8"), on_delivery=on_delivery)
            break
        except BufferError:
            p.poll(0.5)
    p.poll(0)

p.flush()
print(f"DONE: {acked}/{len(lines)} acked to {TOPIC}")
