# Kafka Broker-Failover Demo — Runbook (VM version, 3-laptop split)

Each laptop runs one Ubuntu VM (22.04/24.04). Everything below runs **inside the VMs**.

The point of this setup is one thing: **kill a broker and watch the controller elect new leaders for
its partitions.** The original 4-laptop Kafka + Spark fraud pipeline is still in the repo, but this
runbook only covers the 3 laptops that exist now: the Kafka cluster and the producer.
Postgres has been dropped, so `postgres/` and `ta/verify.py` are **not used** (kept for reference).

## Cluster shape

| Laptop | IP | Runs | Kafka node |
|---|---|---|---|
| A | 172.22.134.139 | Kafka **broker + controller** | node 1 (`kafka/server-node1.properties`) |
| B | 172.22.114.189 | Kafka **broker only** | node 2 (`kafka/server-node2.properties`) |
| C | 172.22.159.246 | **Producer only** (no Kafka) | -- |

- Single controller: `controller.quorum.voters=1@172.22.134.139:9093`. Node 2 is not a voter; it
  registers with node 1's controller.
- Both topics get `num_partitions=4`, `replication_factor=2`. With 2 brokers, every partition has a
  copy on both; leadership is spread roughly 2-and-2. `min.insync.replicas=1` so a partition stays
  writable with one replica left.
- **Kill laptop B, not A.** A holds the controller, and only the controller can elect leaders. Killing
  B leaves the controller alive, so it moves B's partitions' leadership to A. Killing A takes out
  the controller too, and nothing can be re-elected until A is back (see the drill below).
- There is no controller redundancy: a real setup needs 3 controller voters. That needs a 4th machine
  (or a third node on one of these), which is why this demo only shows *broker* failover.

## Networking — read first

ZeroTier (or whatever puts these laptops on 172.22.x.x) must be reachable **from inside each VM**. A
VM behind the hypervisor's NAT is not reachable at the host's IP; use bridged networking or make sure
the VM itself has the 172.22.x.x address. Check: every VM can `ping` the other two, and
`nc -zv 172.22.134.139 9092` / `9093` works from the other laptops once Kafka is up.

IPs are hardcoded in: `kafka/server-node1.properties`, `kafka/server-node2.properties`,
`producer/producer.py`, `producer/create_topic.py`, `spark/fraud_job.py`, `spark/batch_job.py`.
If a laptop's IP changes, edit those (and re-run `setup_kafka.sh` on the Kafka nodes).

## Which files go where

| Laptop | Files |
|---|---|
| A (broker + controller) | `kafka/server-node1.properties`, `kafka/setup_kafka.sh` |
| B (broker) | `kafka/server-node2.properties`, `kafka/setup_kafka.sh` |
| C (producer) | `producer/`, `data/transactions.csv` |
| Not assigned | `spark/` (batch fraud detection + summary; needs a machine with Java + 4 GB RAM -- optional, can run from C after the producer finishes) |
| TA / reference only | `ta/`, `postgres/`, `data/answer_key.csv`, `data/part_A.csv`, `data/part_B.csv` |

## Prerequisites

| Laptop | RAM | Install |
|---|---|---|
| A, B | 2–3 GB | Kafka only: `bash kafka/setup_kafka.sh 1` (A) / `bash kafka/setup_kafka.sh 2` (B) |
| C | 1–2 GB | `python3 -m venv venv && . venv/bin/activate && pip install -r producer/requirements.txt` |

Ubuntu 24.04 blocks system-wide `pip install`, so always use a venv.

## Start-up order

1. **A first**, then **B** (B needs the controller up to register), each in a terminal kept open:
   `KAFKA_HEAP_OPTS='-Xmx512m -Xms512m' /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/fraud.properties`
2. **C**: `python producer/create_topic.py` → should print `brokers: [1, 2]`, then create `transactions`
   and `flagged_transactions` (4 partitions, RF 2) and print each partition's leader/replicas/isr.
   Both brokers should appear as leaders across the partitions.
3. **C**: `python producer/producer.py --csv data/transactions.csv` -- replays the CSV in event-time
   order (use `--speedup N` to go faster). Leave it running for the drill.

## What to check

| Where | Command | Expect |
|---|---|---|
| C | producer terminal | steady `SENT A-000118 card=1009 event=10:07:14 -> p3 off 41` lines |
| A or B | `/opt/kafka/bin/kafka-topics.sh --bootstrap-server 172.22.134.139:9092 --describe --topic transactions` | 4 partitions, leaders split across broker 1 and 2, `Replicas: 1,2`, `Isr: 1,2` |
| A | `/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server 172.22.134.139:9092 describe --status` | `LeaderId: 1`, one voter, broker 2 listed as an observer |

## Failure drill: kill the broker, watch re-election

1. With the producer running, note which partitions are led by broker 2 (`--describe` above).
2. On **B**: `pkill -9 -f kafka.Kafka`.
3. On **A**, re-run `--describe --topic transactions` (use `--bootstrap-server 172.22.134.139:9092`):
   - Partitions formerly led by broker 2 now show `Leader: 1`, `Isr: 1` (ISR shrank; replica 2 is
     still listed under `Replicas` but is out of sync). This is the controller re-electing.
   - The producer may print a short burst of errors/retries, then resumes on those partitions.
     `acks=all` still works because `min.insync.replicas=1`.
4. Restart Kafka on B. Broker 2 rejoins, catches up, and returns to the ISR
   (`Isr: 1,2`). Leadership does not automatically flip back immediately; it does after the
   preferred-leader election (default every 5 minutes), or run:
   `/opt/kafka/bin/kafka-leader-election.sh --bootstrap-server 172.22.134.139:9092 --election-type preferred --all-topic-partitions`
5. **Contrast (optional)**: kill Kafka on **A** instead. The controller dies with it, so no leader
   election can happen: partitions that A led stay leaderless until A returns, and B keeps serving
   only the partitions it already led. That is the cost of a single controller.

## Reset between practice runs

- C: `python producer/create_topic.py --reset` (deletes and recreates both topics)
- If you change `node.id` or the cluster id, wipe `/opt/kafka-data` on that node and re-run `setup_kafka.sh`.
- If you ran Spark: `rm -rf spark/flagged_summary`

## New dataset (TA)

`cd ta && python generate_data.py --seed 1234 --out ../data_demo` → new `part_A.csv`, `part_B.csv`,
`answer_key.csv`. If you regenerate data, remerge into a single sorted file for the producer:

```
python3 -c "
import csv
rows, header = [], None
for f in ['data_demo/part_A.csv', 'data_demo/part_B.csv']:
    with open(f) as fh:
        r = csv.reader(fh); header = next(r); rows.extend(r)
rows.sort(key=lambda r: r[header.index('event_time')])
with open('data_demo/transactions.csv', 'w', newline='') as out:
    w = csv.writer(out); w.writerow(header); w.writerows(rows)
"
```
