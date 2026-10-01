"""VM1: publishes every line of the team's orders.jsonl to Kafka topic `orders`, unchanged.
Malformed / invalid / duplicate lines are sent as-is -- cleaning is Spark Job 1's job.
Usage: python producer.py --file ../data/datasets/team_01/orders.jsonl"""
import argparse
import json
import time

from confluent_kafka import Producer

BOOTSTRAP = "172.22.134.139:9092,172.22.159.246:9092"  # <-- the two Kafka brokers
TOPIC = "orders"

ap = argparse.ArgumentParser()
ap.add_argument("--file", required=True)
ap.add_argument("--delay", type=float, default=0.0, help="seconds between records (e.g. 0.5 to keep it running during the broker-failure test)")
a = ap.parse_args()

p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True, "linger.ms": 5})
acked = 0


def on_delivery(err, msg):
    global acked
    if err:
        print(f"{time.strftime('%H:%M:%S')} FAILED: {err}")
        return
    acked += 1
    print(f"{time.strftime('%H:%M:%S')} SENT p{msg.partition()} off {msg.offset()} {msg.value()[:60].decode(errors='replace')}")


lines = [l.strip() for l in open(a.file, encoding="utf-8") if l.strip()]
for line in lines:
    try:
        key = str(json.loads(line)["order_id"])
    except Exception:
        key = None  # malformed / missing order_id: still published, no key
    while True:
        try:
            p.produce(TOPIC, key=key, value=line.encode("utf-8"), on_delivery=on_delivery)
            break
        except BufferError:
            p.poll(0.5)
    p.poll(0)
    if a.delay:
        time.sleep(a.delay)

p.flush()
print(f"DONE: {acked}/{len(lines)} acked")
