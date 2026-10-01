# Distributed Food Order Processing Pipeline (Kafka KRaft + Spark batch)

| VM | Role | Folder / files |
|---|---|---|
| VM1 (172.22.114.189) | Producer + final consumer + Spark Job 2 | `vm1/`, `data/datasets/<team>/orders.jsonl` |
| VM2 (172.22.134.139) | Kafka broker 1 + KRaft controller | `kafka/server-node1.properties`, `kafka/setup_kafka.sh` |
| VM3 (172.22.159.246) | Kafka broker 2 | `kafka/server-node2.properties`, `kafka/setup_kafka.sh` |
| VM4 (172.22.243.59) | Consumer + Spark Job 1 + producer | `vm4/` |

IPs live in: `kafka/server-node1.properties`, `kafka/server-node2.properties` (`172.22.134.139`, `172.22.159.246`),
and the `BOOTSTRAP` line of every script in `vm1/` and `vm4/`. Every VM must be able to reach VM2 and VM3 on 9092
(and VM3 -> VM2 on 9093). Topics: `orders` and `order_metrics`, 2 partitions, replication factor 2, same cluster.

## Setup
- VM2: `cd kafka && bash setup_kafka.sh 1`    VM3: `cd kafka && bash setup_kafka.sh 2`
- VM1 / VM4: `sudo apt install -y openjdk-17-jre-headless python3-venv`, then
  `python3 -m venv venv && . venv/bin/activate && pip install -r vmX/requirements.txt`
  (if `SPARK_HOME` points at another Spark install, run `unset SPARK_HOME`).

## Run order
1. VM2 then VM3: `KAFKA_HEAP_OPTS='-Xmx512m -Xms512m' /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/pipeline.properties`
2. VM1: `python vm1/create_topics.py` (`--reset` between runs) -- shows leader/replicas/isr for both topics.
3. VM1: `python vm1/producer.py --file data/datasets/team_XX/orders.jsonl` (`--delay 0.5` to stretch it out).
4. VM4: `python vm4/consume_orders.py` -> `orders_landed.jsonl` (stops after 20 s of silence)
5. VM4: `python vm4/spark_job1.py` -> `order_metrics.jsonl`
6. VM4: `python vm4/publish_order_metrics.py` -> topic `order_metrics`
7. VM1: `python vm1/consume_order_metrics.py` -> `order_metrics_landed.jsonl`
8. VM1: `python vm1/spark_job2.py` -> `restaurant_summary.csv`, `city_summary.csv`

Check against the answer key: `python verify.py team_XX --dir <folder with the outputs>`.
(Tested locally for team_01/05/08: all three outputs match.)

## Broker-failure test
1. `kafka-topics.sh --bootstrap-server 172.22.134.139:9092 --describe --topic orders` -- leaders split across brokers 1 and 2, `Replicas: 1,2`, `Isr: 1,2`.
2. Start the producer with `--delay 0.5`; on VM3: `pkill -9 -f kafka.Kafka`.
3. Describe again: all partitions now `Leader: 1`, `Isr: 1`; the producer carries on and the data is still consumable from the surviving broker.
4. Restart Kafka on VM3; ISR returns to `1,2`.

Kill **VM3, not VM2**: VM2 holds the only controller, so killing it stops leader election until it returns.
