# Kafka + Spark Card-Fraud Pipeline — Runbook (VM version, 4-laptop split)

Each laptop runs one Ubuntu VM (22.04/24.04). Everything below runs **inside the VMs**.

This is a restructured version of the original assignment, rebalanced so each of the 4 laptops
does a comparable amount of work, scoped down after faculty feedback that streaming + a broker
cluster + everything else was too much for one assignment. The demo now covers: distributed Kafka
(2-broker cluster, 4 partitions per topic split across both brokers), and **batch** Spark, run
twice, once on each end of the pipe. No Structured Streaming, no checkpointing -- both Spark jobs
read a fixed offset range (earliest..latest) and exit when done.
Postgres has been dropped -- results live entirely in Kafka + a Spark-written CSV report, so
`postgres/` and `ta/verify.py` are **not used** in this flow (kept only for reference).

## Pipeline shape

```
L1 (producer) --> transactions topic --> L4 (batch Spark #1, detects fraud)
    (via L2, L3 brokers)                        |
                                                 v
L1 (batch Spark #2, summarizes) <-- flagged_transactions topic <-- L4 (republishes alerts)
    (via L2, L3 brokers)
```

L2 and L3 are pure Kafka brokers for both topics -- nothing else runs there. Both topics get
`num_partitions=4`, `replication_factor=2` (the max possible with 2 brokers): Kafka's own
partitioner spreads messages across the 4 partitions by key (`card_id`), and its leader-election
spreads partition *leadership* roughly 2-and-2 across L2 and L3 automatically. This isn't
something you configure in the dataset -- it's purely a topic-creation setting (see
`producer/create_topic.py`).

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
| L1 | Producer (raw txns), then Batch Spark #2 (alert summary) | `producer/`, `spark/batch_job.py`, `spark/requirements.txt`, `data/transactions.csv` |
| L2 | Kafka node 2 | `kafka/server-L2.properties`, `kafka/setup_kafka.sh` |
| L3 | Kafka node 3 | `kafka/server-L3.properties`, `kafka/setup_kafka.sh` |
| L4 | Batch Spark #1 (fraud detection + republish) | `spark/fraud_job.py`, `spark/rules.py`, `spark/requirements.txt` |
| TA / reference only | data generation, answer key, original per-broker split | `ta/`, `postgres/`, `data/answer_key.csv`, `data/part_A.csv`, `data/part_B.csv` |

## Prerequisites / VM sizing

| VM | RAM | Install |
|---|---|---|
| L1 | 3–4 GB | Python venv with `pip install -r producer/requirements.txt` (light, for the producer step) **and** `sudo apt install openjdk-17-jdk python3-venv` + `pip install -r spark/requirements.txt` (for the batch step). These two only ever run one at a time -- close the producer process before starting the batch job, so you're never paying for both simultaneously. |
| L2, L3 | 2–3 GB | Kafka only (`bash setup_kafka.sh 2` / `3`) |
| L4 | 4 GB+ | `sudo apt install openjdk-17-jdk python3-venv`, then `python3 -m venv venv && . venv/bin/activate && pip install -r spark/requirements.txt`. Internet on first run (Spark fetches the Kafka connector jar). |

Ubuntu 24.04 blocks system-wide `pip install`, so always use a venv.

**Note on L1's RAM**: L1 is now the only VM doing two different jobs. If your VM is tight on RAM
(under ~4GB total), make sure the producer process has fully exited (`DONE: n/n acked` printed,
terminal back at a prompt) before starting the Spark JVM for `batch_job.py` -- don't run them
side by side.

## Start-up order

Both Spark jobs are one-shot batch reads over a fixed offset range (earliest..latest at the moment
you run them) -- so order matters: each side must finish producing into a topic before the other
side reads it, or the reader will just see a partial (or empty) snapshot and exit having missed
data produced after it started.

1. **L2, L3**, within about a minute of each other (2-node quorum needs both), in a terminal kept open:
   `KAFKA_HEAP_OPTS='-Xmx512m -Xms512m' /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/fraud.properties`
2. **L1**: `python create_topic.py` → brokers `[2, 3]`, creates `transactions` and
   `flagged_transactions`, both 4 partitions / replication factor 2.
3. **L1**: `python producer.py --csv data/transactions.csv` -- sends all raw txns. Wait for
   `DONE: n/n acked` before continuing. (`--start-at HH:MM:SS` still works if you want a scheduled
   demo start, and `--speedup N` to replay faster -- neither is required for syncing with another
   producer anymore, since L1 is now the only one.)
4. **L4**: once L1's producer shows `DONE`, `python fraud_job.py` -- reads all of `transactions`
   in one shot, runs the fraud rules per card, prints alerts, republishes them onto
   `flagged_transactions`, then exits.
5. **L1**: once L4's job has finished and exited, `python spark/batch_job.py` -- reads all of
   `flagged_transactions` in one shot, prints a summary (counts by rule, top offending cards), and
   writes `spark/flagged_summary/*.csv`.

## What to check during the run

| Where | Command / place | Expect |
|---|---|---|
| L1 | producer terminal | steady `SENT A-000118 card=1009 event=10:07:14 -> p3 off 41` lines, `DONE: n/n acked` |
| any Kafka VM | `/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning` | all produced messages, spread across 4 partitions |
| any Kafka VM | `/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions` | 4 partitions, leaders split across broker 2 and broker 3, 2 replicas (ISR) each |
| L4 | terminal | `N alerts detected across all cards` + `ALERT R1:A-000118 card 1009 ...` lines, then exits |
| any Kafka VM | `/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flagged_transactions --from-beginning` | alert JSON, one per detected rule violation |
| L1 | batch job terminal | rule counts, top-10 cards, `wrote per-alert detail to ./flagged_summary/` |

## Failure drills (during the run)

- **Broker kill**, on L2 or L3 mid-produce or mid-batch-read: `pkill -9 -f kafka.Kafka`. Whichever
  Spark/producer job is talking to Kafka at that moment logs errors/retries and (if `acks=all` /
  `min.insync.replicas=1` are satisfied by the remaining broker) carries on. Note: with only 2
  Kafka nodes, killing either one **does** lose controller quorum (2-of-2), unlike a 3-node cluster
  which survives losing 1 -- worth calling out explicitly during the demo as the tradeoff of
  splitting Kafka duty across only 2 laptops.
- **Rerunning a batch job**: if `fraud_job.py` or `batch_job.py` is interrupted partway, just rerun
  it -- each is a full, fresh read of its input topic's current offset range. The one thing to
  watch: rerunning `fraud_job.py` after a prior successful run will **re-publish duplicate alert
  messages** onto `flagged_transactions` (there's no dedup on that write, unlike the old Postgres
  upsert). If you need a clean rerun, `--reset` the topics first (below).

## Reset between practice runs

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
