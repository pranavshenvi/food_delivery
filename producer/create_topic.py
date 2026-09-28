"""Run once from the producer laptop after both brokers are up.  --reset deletes both topics first (between practice runs).
Creates `transactions` (raw feed, producer -> Spark) and `flagged_transactions` (alerts, Spark -> producer laptop)."""
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "172.22.134.139:9092,172.22.114.189:9092"  # <-- the two Kafka brokers

TOPICS = [
    NewTopic("transactions", num_partitions=4, replication_factor=2, config={"min.insync.replicas": "1"}),
    NewTopic("flagged_transactions", num_partitions=4, replication_factor=2, config={"min.insync.replicas": "1"}),
]

admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
print("brokers:", sorted(b.id for b in admin.list_topics(timeout=10).brokers.values()))

if "--reset" in sys.argv:
    for f in admin.delete_topics([t.topic for t in TOPICS]).values():
        try:
            f.result()
            print("deleted", f)
        except Exception as e:
            print("delete:", e)
    time.sleep(3)

for name, f in admin.create_topics(TOPICS).items():
    try:
        f.result()
        print("created", name)
    except Exception as e:
        print(name, e)
time.sleep(1)

for t in TOPICS:
    md = admin.list_topics(t.topic, timeout=10).topics[t.topic]
    print(f"--- {t.topic} ---")
    for pid, part in sorted(md.partitions.items()):
        print(f"p{pid}: leader={part.leader} replicas={part.replicas} isr={part.isrs}")
