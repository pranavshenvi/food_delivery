# Kafka + Spark Card-Fraud Pipeline — Runbook (VM version)

Each laptop runs one Ubuntu VM (22.04/24.04). Everything below runs **inside the VMs**.

## Networking — read first

ZeroTier must be installed and joined **inside each VM**, not only on the host laptop. A VM behind the
hypervisor's NAT is not reachable at the host's ZeroTier IP. Each VM gets its own ZeroTier IP, and each
must be authorized in ZeroTier Central. Check: every VM can `ping` every other VM's ZeroTier IP.

Placeholder IPs used in the files (replace everywhere): L1 = 10.147.17.11, L2 = 10.147.17.12,
L3 = 10.147.17.13, subnet 10.147.17.0/24. Files to edit: `kafka/server-L*.properties`,
`postgres/setup_postgres.sh`, `producer/producer.py`, `producer/create_topic.py`, `spark/fraud_job.py`, `ta/verify.py`.

## Which files go where

| VM | Runs | Files |
|---|---|---|
| L1 | Kafka node 1, Producer A | `kafka/`, `producer/`, `data/part_A.csv` |
| L2 | Kafka node 2, Producer B | `kafka/`, `producer/producer.py`, `producer/requirements.txt`, `data/part_B.csv` |
| L3 | Kafka node 3, Postgres | `kafka/`, `postgres/` |
| L4 | Spark job | `spark/` |
| TA only | data generation, answer key, grading | `ta/`, `data/answer_key.csv` |

## Prerequisites / VM sizing

| VM | RAM | Install |
|---|---|---|
| L1, L2 | 2–3 GB | Kafka (`bash setup_kafka.sh 1` / `2`), Python venv with `pip install -r requirements.txt` |
| L3 | 3 GB | Kafka (`bash setup_kafka.sh 3`), Postgres (`bash setup_postgres.sh`) |
| L4 | 4 GB+ | `sudo apt install openjdk-17-jdk python3-venv`, then `python3 -m venv venv && . venv/bin/activate && pip install -r requirements.txt`. Internet on first run (Spark fetches the Kafka connector jar). |

Ubuntu 24.04 blocks system-wide `pip install`, so always use a venv on L1, L2, L4.

## Start-up order

1. **L1, L2, L3** within about a minute of each other (the controller quorum needs 2 of 3), in a terminal kept open:
   `KAFKA_HEAP_OPTS='-Xmx512m -Xms512m' /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/fraud.properties`
2. **L3**: Postgres is already running as a service after setup (`sudo systemctl status postgresql`).
3. **L1**: `python create_topic.py` → brokers `[1, 2, 3]`, 6 partitions, 3 replicas each.
4. **L4**: `python fraud_job.py` → waits for data, prints a progress line every 10 s.
5. **L1 and L2**, same start time:
   `python producer.py --csv part_A.csv --start-at 14:30:00` (L1)
   `python producer.py --csv part_B.csv --start-at 14:30:00` (L2)
   The run lasts 15 minutes; `--speedup 3` makes it 5. VM clocks must agree (`timedatectl` → NTP synchronized: yes).

## What the TA checks

| Where | Command / place | Expect |
|---|---|---|
| L1, L2 | producer terminal | steady `SENT A-000118 card=1009 event=10:07:14 -> p3 off 41` lines, `DONE: 213/213 acked` |
| any Kafka VM | `/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions` | messages flowing live |
| any Kafka VM | `/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions` | leaders/ISR; after a broker kill, ISR shrinks and leaders move |
| L3 | `PGPASSWORD=fraud psql -h localhost -U fraud -d fraud -c "SELECT rule, count(*) FROM alerts GROUP BY rule;"` | counts grow during the run |
| L4 | terminal | `Batch 57: 1 alerts` + `ALERT R1:A-000118 card 1009 ...` |
| any | `http://<L4-ZeroTier-IP>:4040` → Structured Streaming tab | input rate, batch IDs increasing |
| TA | `python ta/verify.py --key data/answer_key.csv` | `RESULT: PASS` |

## Failure drills (during the run)

- **Broker kill** (~5 min in), on L2: `pkill -9 -f kafka.Kafka`. Producers log a few errors/retries and carry on; nothing is lost. Restart later with the start command.
- **Spark kill** (~8 min in), on L4: `pkill -9 -f fraud_job.py`, wait 30 s, rerun `python fraud_job.py`. It resumes from `./checkpoint`; a replayed batch prints `(n already in Postgres, skipped)`.

## Reset between practice runs

- L4: `rm -rf checkpoint`
- L3: `PGPASSWORD=fraud psql -h localhost -U fraud -d fraud -c "TRUNCATE alerts;"`
- L1: `python create_topic.py --reset`

## New dataset (TA)

`cd ta && python generate_data.py --seed 1234 --out ../data_demo` → new `part_A.csv`, `part_B.csv`, `answer_key.csv`.
