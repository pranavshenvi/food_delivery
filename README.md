# Kafka + Spark Card-Fraud Pipeline — Runbook (VM version, 4-laptop split)

Each laptop runs one Ubuntu VM (22.04/24.04). Everything below runs **inside the VMs**.

This is a restructured version of the original assignment, rebalanced so each of the 4 laptops
does a comparable amount of work and the demo covers: distributed Kafka (2-broker cluster),
**streaming** Spark (L4), **batch** Spark (L1), and Structured Streaming checkpointing.
Postgres has been dropped -- results live entirely in Kafka + a Spark-written CSV report, so
`postgres/` and `ta/verify.py` are **not used** in this flow (kept only for reference).

## Pipeline shape

```
L1 (producer) --> transactions topic --> L4 (streaming Spark, detects fraud)
                                              |
                                              v
L1 (batch Spark, summarizes) <-- flagged_transactions topic <-- L4 (republishes alerts)
```

L2 and L3 are pure Kafka brokers for both topics. L1 flips from producer to consumer partway
through the run; L4 flips from consumer to producer (of alerts) as part of the same job.

## Networking — read first

ZeroTier must be installed and joined **inside each VM**, not only on the host laptop. A VM behind
the hypervisor's NAT is not reachable at the host's ZeroTier IP. Each VM gets its own ZeroTier IP,
and each must be authorized in ZeroTier Central. Check: every VM can `ping` every other VM's
ZeroTier IP.

Only L2 and L3's IPs are ever hardcoded in config (they're the only brokers). Placeholder IPs used
in the files (replace everywhere): L2 = 10.147.17.12, L3 = 10.147.17.13. Files to edit:
`kafka/server-L2.properties`, `kafka/server-L3.properties`, `producer/producer.py`,
`producer/create_topic.py`, `spark/fraud_job.py`, `spark/batch_job.py`.

## Which files go where

| VM | Runs | Files |
|---|---|---|
| L1 | Producer (raw txns), then Batch Spark (alert summary) | `producer/`, `spark/batch_job.py`, `spark/requirements.txt`, `data/transactions.csv` |
| L2 | Kafka node 2 | `kafka/server-L2.properties`, `kafka/setup_kafka.sh` |
| L3 | Kafka node 3 | `kafka/server-L3.properties`, `kafka/setup_kafka.sh` |
| L4 | Streaming Spark (fraud detection + republish) | `spark/fraud_job.py`, `spark/rules.py`, `spark/requirements.txt` |
| TA / reference only | data generation, answer key, original per-broker split | `ta/`, `postgres/`, `data/answer_key.csv`, `data/part_A.csv`, `data/part_B.csv` |

## Prerequisites / VM sizing

| VM | RAM | Install |
|---|---|---|
| L1 | 3–4 GB | Python venv with `pip install -r producer/requirements.txt` (light, for the producer step) **and** `sudo apt install openjdk-17-jdk python3-venv` + `pip install -r spark/requirements.txt` (for the batch step). These two only ever run one at a time -- close the producer venv/process before starting the batch job, so you're never paying for both simultaneously. |
| L2, L3 | 2–3 GB | Kafka only (`bash setup_kafka.sh 2` / `3`) |
| L4 | 4 GB+ | `sudo apt install openjdk-17-jdk python3-venv`, then `python3 -m venv venv && . venv/bin/activate && pip install -r spark/requirements.txt`. Internet on first run (Spark fetches the Kafka connector jar). |

Ubuntu 24.04 blocks system-wide `pip install`, so always use a venv.

**Note on L1's RAM**: L1 is now the only VM doing two different jobs. If your VM is tight on RAM
(under ~4GB total), make sure the producer process has fully exited (`DONE: n/n acked` printed,
terminal back at a prompt) before starting the Spark JVM for `batch_job.py` -- don't run them
side by side.

## Start-up order

1. **L2, L3**, within about a minute of each other (2-node quorum needs both), in a terminal kept open:
   `KAFKA_HEAP_OPTS='-Xmx512m -Xms512m' /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/fraud.properties`
2. **L1**: `python create_topic.py` → brokers `[2, 3]`, creates `transactions` (6 partitions) and
   `flagged_transactions` (3 partitions), both replication factor 2.
3. **L4**: `python fraud_job.py` → waits for data, prints a progress line every 10 s.
4. **L1**: `python producer.py --csv data/transactions.csv --start-at 14:30:00`
   The run lasts 15 minutes; `--speedup 3` makes it 5. VM clocks must agree
   (`timedatectl` → NTP synchronized: yes).
5. Once the producer prints `DONE: n/n acked`, wait ~20–30s and watch L4's progress lines settle
   (input rows per batch dropping to 0) -- that means it's caught up and drained the backlog.
6. **L1**: `python spark/batch_job.py` -- one-shot batch read of `flagged_transactions`, prints a
   summary (counts by rule, top offending cards) and writes `spark/flagged_summary/*.csv`.

## What to check during the run

| Where | Command / place | Expect |
|---|---|---|
| L1 | producer terminal | steady `SENT A-000118 card=1009 event=10:07:14 -> p3 off 41` lines, `DONE: n/n acked` |
| any Kafka VM | `/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions` | messages flowing live |
| any Kafka VM | `/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flagged_transactions` | alert JSON flowing as L4 detects fraud |
| any Kafka VM | `/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions` | leaders/ISR; after a broker kill, ISR shrinks and leader moves |
| L4 | terminal | `Batch 57: 1 alerts` + `ALERT R1:A-000118 card 1009 ...` |
| any | `http://<L4-ZeroTier-IP>:4040` → Structured Streaming tab | input rate, batch IDs increasing |
| L1 | batch job terminal | rule counts, top-10 cards, `wrote per-alert detail to ./flagged_summary/` |

## Failure drills (during the run)

- **Broker kill** (~5 min in), on L2 or L3: `pkill -9 -f kafka.Kafka`. Producer logs a few
  errors/retries and carries on; nothing acked is lost. Note: with only 2 Kafka nodes, killing
  either one **does** lose controller quorum (2-of-2), unlike a 3-node cluster which survives
  losing 1. Good to call out explicitly during the demo as the tradeoff of the 4-way rebalance.
  Restart later with the start command.
- **Spark kill** (~8 min in), on L4: `pkill -9 -f fraud_job.py`, wait 30 s, rerun
  `python fraud_job.py`. It resumes from `./checkpoint`; a replayed batch republishes the same
  alerts (Kafka dedup isn't automatic here the way the old Postgres upsert was, so mention this
  as a known limitation of the simplified sink if it comes up in the demo).

## Reset between practice runs

- L4: `rm -rf checkpoint`
- L1: `python create_topic.py --reset` (deletes and recreates both topics)
- L1: `rm -rf spark/flagged_summary`

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
