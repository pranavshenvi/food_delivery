"""Run once from L1 after all 3 brokers are up.  --reset deletes the topic first (between practice runs)."""
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = "10.147.17.11:9092,10.147.17.12:9092,10.147.17.13:9092"  # <-- ZeroTier IPs of L1, L2, L3 VMs

admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
print("brokers:", sorted(b.id for b in admin.list_topics(timeout=10).brokers.values()))
if "--reset" in sys.argv:
    for f in admin.delete_topics(["transactions"]).values():
        try:
            f.result()
            print("deleted transactions")
        except Exception as e:
            print("delete:", e)
    time.sleep(3)
topic = NewTopic("transactions", num_partitions=6, replication_factor=3, config={"min.insync.replicas": "2"})
for name, f in admin.create_topics([topic]).items():
    try:
        f.result()
        print("created", name)
    except Exception as e:
        print(name, e)
time.sleep(1)
md = admin.list_topics("transactions", timeout=10).topics["transactions"]
for pid, part in sorted(md.partitions.items()):
    print(f"p{pid}: leader={part.leader} replicas={part.replicas} isr={part.isrs}")
